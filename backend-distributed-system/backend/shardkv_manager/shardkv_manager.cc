#include <grpcpp/grpcpp.h>

#include "shardkv_manager.h"
#include "../build/shardkv.grpc.pb.h"

::grpc::Status ShardkvManager::Get(::grpc::ServerContext* context,
    const ::GetRequest* request,
    ::GetResponse* response) {


    std::unique_lock<std::mutex> lock(this->skvMutex);

    auto channel = ::grpc::CreateChannel(this->primaryAddress, ::grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    ::grpc::ClientContext cc;

    auto status = kvStub->Get(&cc, *request, response);

    if (status.ok()) {
        std::cout << "SUCCESSr" << std::endl;
    }
    else {
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "FAILED");
    }

    lock.unlock();
    return ::grpc::Status::OK;
}


::grpc::Status ShardkvManager::Put(::grpc::ServerContext* context,
    const ::PutRequest* request,
    Empty* response) {

    std::unique_lock<std::mutex> lock(this->skvMutex);

    auto channel = ::grpc::CreateChannel(this->primaryAddress, ::grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    ::grpc::ClientContext cc;

    auto status = kvStub->Put(&cc, *request, response);

    if (status.ok()) {
        std::cout << "SUCCESS" << std::endl;
    }
    else {
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Shardmanager FAILED");
    }

    lock.unlock();
    return ::grpc::Status::OK;
}


::grpc::Status ShardkvManager::Append(::grpc::ServerContext* context,
    const ::AppendRequest* request,
    Empty* response) {

    std::unique_lock<std::mutex> lock(this->skvMutex);

    auto channel = ::grpc::CreateChannel(this->primaryAddress, ::grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    ::grpc::ClientContext cc;

    auto status = kvStub->Append(&cc, *request, response);

    if (status.ok()) {
        std::cout << "SUCCESS" << std::endl;
    }
    else {
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Shardmanager FAILED");
    }

    lock.unlock();
    return ::grpc::Status::OK;
}


::grpc::Status ShardkvManager::Delete(::grpc::ServerContext* context,
    const ::DeleteRequest* request,
    Empty* response) {

    std::unique_lock<std::mutex> lock(this->skvMutex);

    auto channel = ::grpc::CreateChannel(this->primaryAddress, ::grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    ::grpc::ClientContext cc;

    auto status = kvStub->Delete(&cc, *request, response);

    if (status.ok()) {
        std::cout << "SUCCESS" << std::endl;
    }
    else {
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Shardmanager FAILED");
    }

    if (!this->backupAddress.empty()) {

        auto channel2 = ::grpc::CreateChannel(this->backupAddress, ::grpc::InsecureChannelCredentials());
        auto kvStub2 = Shardkv::NewStub(channel2);
        ::grpc::ClientContext cc2;

        auto status2 = kvStub2->Delete(&cc2, *request, response);

        if (status2.ok()) {
            std::cout << "SUCCESS" << std::endl;
        }
        else {
            lock.unlock();
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Shardmanager FAILED");
        }
    }

    lock.unlock();
    return ::grpc::Status::OK;
}

::grpc::Status ShardkvManager::Ping(::grpc::ServerContext* context, const PingRequest* request,
    ::PingResponse* response) {

    std::unique_lock<std::mutex> lock(this->skvMutex);
    std::string skvAddress = request->server();
    std::vector<std::string> newSkv;

    if (this->primaryAddress.empty()) {

        this->primaryAddress = skvAddress;
        this->lastAckView = currentView;
        this->currentView++;
        newSkv.push_back(this->primaryAddress);
        newSkv.push_back("");
        response->set_id(this->currentView);
        this->views[this->currentView] = newSkv;
        response->set_primary(skvAddress);
        response->set_backup("");

    }
    else if ((this->backupAddress.empty()) && (skvAddress != this->primaryAddress)) {

        this->backupAddress = skvAddress;
        this->currentView++;
        newSkv.push_back(this->primaryAddress);
        newSkv.push_back(skvAddress);
        this->views[this->currentView] = newSkv;
        response->set_id(this->lastAckView);
        response->set_primary(this->primaryAddress);
        response->set_backup(skvAddress);

    }
    else if (this->primaryAddress == skvAddress) {

        this->lastAckView = request->viewnumber();
        response->set_primary(this->primaryAddress);
        response->set_backup(this->backupAddress);
        response->set_id(this->currentView);

    }
    else if (this->backupAddress == skvAddress) {

        std::string primary = this->views[this->lastAckView].at(0);
        std::string backup = this->views[this->lastAckView].at(1);

        response->set_primary(primary);
        response->set_backup(backup);
        response->set_id(this->lastAckView);
    }
    else {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "SERVERS OVERLOADED");
    }

    PingInterval ping;
    ping.Push(std::chrono::high_resolution_clock::now());
    this->pingIntervals[skvAddress] = ping;

    response->set_shardmaster(this->smAddress);

    lock.unlock();
    return ::grpc::Status(::grpc::StatusCode::OK, "SUCCESS");
}
