#include "shardmaster.h"
#include <vector>

::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
    std::lock_guard<std::mutex> lock(ssm_mtx);

    const auto& server_id = request->server();
    if(ssm.find(server_id) != ssm.end()){
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server already exists");
    }

    ser.push_back(server_id);
    ssm[server_id] = {};

    int total_servers = ssm.size();
    int key_range = MAX_KEY - MIN_KEY + 1;
    int keys_per_server = key_range / total_servers;
    int extra_keys = key_range % total_servers;

    int lower_bound = MIN_KEY;
    for(auto& server : ser){
        int upper_bound = lower_bound + keys_per_server - 1;
        if(extra_keys > 0){
            upper_bound++;
            extra_keys--;
        }

        ssm[server] = {shard_t{lower_bound, upper_bound}};
        lower_bound = upper_bound + 1;
    }

    return ::grpc::Status::OK;
}


::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext* context,
                                        const ::LeaveRequest* request,
                                        Empty* response) {
    std::lock_guard<std::mutex> lock(ssm_mtx);

    int total_keys = MAX_KEY - MIN_KEY + 1;

    for(int i = 0; i < request->servers_size(); ++i){
        const auto& server_id = request->servers(i);
        if(ssm.find(server_id) == ssm.end()){
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not found");
        }

        ssm.erase(server_id);
        auto it = std::find(ser.begin(), ser.end(), server_id);
        if (it != ser.end()) ser.erase(it);
    }

    int total_servers = ssm.size();
    int keys_per_server = total_keys / total_servers;
    int extra_keys = total_keys % total_servers;

    int lower_bound = MIN_KEY;
    for(auto& server : ser){
        int upper_bound = lower_bound + keys_per_server - 1;
        if(extra_keys > 0){
            upper_bound++;
            extra_keys--;
        }

        ssm[server] = {shard_t{lower_bound, upper_bound}};
        lower_bound = upper_bound + 1;
    }

    return ::grpc::Status::OK;
}


::grpc::Status StaticShardmaster::Move(::grpc::ServerContext* context,
                                       const ::MoveRequest* request,
                                       Empty* response) {
    std::lock_guard<std::mutex> lock(ssm_mtx);

    const auto& target_server = request->server();
    if(ssm.find(target_server) == ssm.end()){
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not found");
    }

    shard_t requested_shard;
    requested_shard.lower = request->shard().lower();
    requested_shard.upper = request->shard().upper();

    for(auto& [server, shards] : ssm){
        std::vector<shard_t> updated_shards;
        for(auto& shard : shards){
            auto overlap_status = get_overlap(shard, requested_shard);

            switch(overlap_status){
                case OverlapStatus::OVERLAP_START:
                    updated_shards.push_back(shard_t{requested_shard.upper + 1, shard.upper});
                    break;
                case OverlapStatus::OVERLAP_END:
                    updated_shards.push_back(shard_t{shard.lower, requested_shard.lower - 1});
                    break;
                case OverlapStatus::COMPLETELY_CONTAINS:
                    updated_shards.push_back(shard_t{shard.lower, requested_shard.lower - 1});
                    updated_shards.push_back(shard_t{requested_shard.upper + 1, shard.upper});
                    break;
                case OverlapStatus::NO_OVERLAP:
                    updated_shards.push_back(shard);
                    break;
            }
        }

        ssm[server] = updated_shards;
    }

    ssm[target_server].push_back(requested_shard);
    sortAscendingInterval(ssm[target_server]);

    return ::grpc::Status::OK;
}


::grpc::Status StaticShardmaster::Query(::grpc::ServerContext* context,
                                        const Empty* request,
                                        ::QueryResponse* response) {
    std::lock_guard<std::mutex> lock(ssm_mtx);

    for(const auto& server : ser){
        auto* conf_entry = response->add_config();
        conf_entry->set_server(server);
        for(const auto& shard : ssm[server]){
            auto* shard_entry = conf_entry->add_shards();
            shard_entry->set_lower(shard.lower);
            shard_entry->set_upper(shard.upper);
        }
    }

    return ::grpc::Status::OK;
}
