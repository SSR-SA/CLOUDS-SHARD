#ifndef SHARDING_SHARDMASTER_H
#define SHARDING_SHARDMASTER_H

#include "../common/common.h"
#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include <string>
#include <vector>
#include <mutex>
#include "../build/shardmaster.grpc.pb.h"

/**
 * @brief Class representing a Static Shardmaster for managing sharding in a distributed system.
 * It provides functionalities for joining, leaving, moving, and querying shards.
 */
class StaticShardmaster : public Shardmaster::Service {
    using Empty = google::protobuf::Empty;

public:
    // Override methods from Shardmaster::Service
    ::grpc::Status Join(::grpc::ServerContext* context,
                        const ::JoinRequest* request, Empty* response) override;
    ::grpc::Status Leave(::grpc::ServerContext* context,
                         const ::LeaveRequest* request, Empty* response) override;
    ::grpc::Status Move(::grpc::ServerContext* context,
                        const ::MoveRequest* request, Empty* response) override;
    ::grpc::Status Query(::grpc::ServerContext* context, const Empty* request,
                         ::QueryResponse* response) override;

private:
    std::mutex ssm_mtx; // Mutex for thread-safe operations on shard data structures
    std::unordered_map<std::string, std::vector<shard_t>> ssm; // Map of server IDs to shard ranges
    std::vector<std::string> ser; // List of server IDs for ordered traversal
};

#endif // SHARDING_SHARDMASTER_H
