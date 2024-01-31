#ifndef SHARDING_SHARDKV_H
#define SHARDING_SHARDKV_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "../common/common.h"
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <fstream>

#include "../build/shardkv.grpc.pb.h"
#include "../build/shardmaster.grpc.pb.h"

class ShardkvServer : public Shardkv::Service {
  using Empty = google::protobuf::Empty;

 public:
  explicit ShardkvServer(std::string addr, const std::string& shardmanager_addr)
      : address(std::move(addr)), shardmanager_address(shardmanager_addr) {

    std::thread query(
            [this]() {
                std::chrono::milliseconds timespan(100);
                while (shardmaster_address.empty() && (this->primary_address != this->address)) {
                    std::this_thread::sleep_for(timespan);
                }
                auto stub = Shardmaster::NewStub(
                        grpc::CreateChannel(shardmaster_address, grpc::InsecureChannelCredentials()));
                while (true) {
                    this->QueryShardmaster(stub.get());
                    std::this_thread::sleep_for(timespan);
                }
            }
            );
    query.detach();


    std::thread heartbeat(
        [this](const std::string sm_addr) {
            std::chrono::milliseconds timespan(100);
            auto stub = Shardkv::NewStub(
                    grpc::CreateChannel(sm_addr, grpc::InsecureChannelCredentials()));
            while (true) {
                PingShardmanager(stub.get());
                std::this_thread::sleep_for(timespan);
            }
        },
        shardmanager_addr);
    heartbeat.detach();
  };

  int32_t MAX_TRIAL = 1000;

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
    ::grpc::Status Dump(::grpc::ServerContext* context,
                        const ::google::protobuf::Empty* request,
                        ::DumpResponse* response);

  void QueryShardmaster(Shardmaster::Stub* stub);

  void PingShardmanager(Shardkv::Stub* stub);

 private:
  const std::string address;
  std::string shardmanager_address;
  std::string shardmaster_address;
  
  std::map<std::string, std::string> database;
  
  std::map<int, std::string> key_server;
  
  std::map<std::string, std::string> post_usr;
  
  std::mutex skv_mtx;

  int64_t viewnumber;

  std::string backup_address;
  
  std::string primary_address;

};

#endif