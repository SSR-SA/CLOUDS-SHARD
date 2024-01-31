#ifndef SHARDING_SHARDKV_MANAGER_H
#define SHARDING_SHARDKV_MANAGER_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "../common/common.h"
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <fstream>
#include <map> // Include the map header for std::map

#include "../build/shardkv.grpc.pb.h"
#include "../build/shardmaster.grpc.pb.h"

// Declare PingInterval class here
class PingInterval {
    std::chrono::time_point<std::chrono::system_clock> time;

public:
    std::uint64_t GetPingInterval() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch() - time.time_since_epoch()
        ).count();
    };

    void Push(std::chrono::time_point<std::chrono::system_clock> t) {
        time = t;
    }
};

class ShardkvManager : public Shardkv::Service {
    using Empty = google::protobuf::Empty;

public:
    explicit ShardkvManager(std::string serverAddress, const std::string& shardmasterAddress)
        : address(std::move(serverAddress)), smAddress(shardmasterAddress) {
        // TODO: Part 3
        // This thread will query the shardmaster every 1 second for updates
        std::thread heartbeatChecker(
            [this]() {
                std::chrono::milliseconds heartbeatInterval(1000);
                while (primaryAddress.empty()) {
                    std::this_thread::sleep_for(heartbeatInterval);
                }

                while (true) {
                    std::this_thread::sleep_for(heartbeatInterval);
                    PingInterval ping = pingIntervals[this->primaryAddress];

                    if (ping.GetPingInterval() > this->deadPingInterval && !this->primaryAddress.empty()) {

                        this->skvMutex.lock();

                        std::cout << "LOST PRIMARY SERVER" << std::endl;
                        this->primaryAddress = this->backupAddress;
                        this->backupAddress = "";
                        this->currentView = this->lastAckView + 1;
                        this->views[this->currentView].push_back(this->primaryAddress);
                        this->views[this->currentView].push_back(this->backupAddress);

                        this->skvMutex.unlock();

                    }
                    /*
                    if(bp.GetPingInterval() > this->deadPingInterval && this->shardKV_backup_address != ""){
                        
                        this->skv_mtx.lock();

                        std::cout << "LOST PRIMARY SERVER" << std::endl;
                        this->shardKV_backup_address = "";
                        this->current_view = this->last_ack_view+1;
                        this->views[this->current_view].push_back(this->shardKV_address);
                        this->views[this->current_view].push_back(this->shardKV_backup_address);

                        this->skv_mtx.unlock();
                    }
                    */
                }
            });
        // We detach the thread so we don't have to wait for it to terminate later
        heartbeatChecker.detach();
    };

    // TODO implement these three methods
    ::grpc::Status Get(::grpc::ServerContext* context,
        const ::GetRequest* request,
        ::GetResponse* response) override;
    ::grpc::Status Put(::grpc::ServerContext* context,
        const ::PutRequest* request, Empty* response) override;
    ::grpc::Status Append(::grpc::ServerContext* context,
        const ::AppendRequest* request,
        Empty* response) override;
    ::grpc::Status Delete(::grpc::ServerContext* context,
        const ::DeleteRequest* request,
        Empty* response) override;
    ::grpc::Status Ping(::grpc::ServerContext* context, const PingRequest* request,
        ::PingResponse* response) override;

private:
    // address we're running on (hostname:port)
    const std::string address;

    // shardmaster address
    std::string smAddress;

    // mutex
    std::mutex skvMutex;

    // address of the primary server
    std::string primaryAddress;

    // address of the backup server
    std::string backupAddress;

    // current view number
    int64_t currentView = 0;

    // last acknowledged view
    int64_t lastAckView = 0;

    // the intervals of ping
    std::map<std::string, PingInterval> pingIntervals;

    // views
    std::map<int, std::vector<std::string>> views;

    // time over which consider a server dead
    uint64_t deadPingInterval = 2000;
    // TODO add any fields you want here!
};
#endif  // SHARDING_SHARDKV_MANAGER_H

