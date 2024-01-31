#include <grpcpp/grpcpp.h>

#include "shardkv.h"


::grpc::Status ShardkvServer::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
  
  std::string key = request->key();
  
  if(this->database.find(key)==this->database.end()){
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Key not found!");
  }

  const std::string &val = database[key];
  response->set_data(val);
  return ::grpc::Status::OK;
}

::grpc::Status ShardkvServer::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {


    std::string key_req = request->key();
    std::string data_req = request->data();
    std::string user_req = request->user();

    
    if(this->primary_address == this->address){

        if (!this->backup_address.empty()){
            
            auto channel = ::grpc::CreateChannel(this->backup_address, ::grpc::InsecureChannelCredentials());
            auto kvStub2 = Shardkv::NewStub(channel);
            ::grpc::ClientContext cc;
            
            auto status = kvStub2->Put(&cc, *request, response);

            if (status.ok()){
                std::cout << "SUCCESS" << std::endl;
            }else{
                return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Shardmanager FAILED");
            }

        }
    }

    int key_id = extractID(key_req);

    if(this->key_server.find(key_id)==this->key_server.end() || this->key_server[key_id]!=this->shardmanager_address){
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not responsible for the Key!");
    }

    if(this->database.find(key_req)==this->database.end()){
        
        
        if(key_req.find("post", 0) != std::string::npos){
            
            if(user_req!=""){
                
                int uid = extractID(user_req);
                
                if(this->key_server[uid]==this->shardmanager_address)
                    
                    this->database[user_req+"_posts"] += (key_req+",");
                
                else{
                    
                    std::chrono::milliseconds timespan(100);
                    auto channel = grpc::CreateChannel(key_server[uid], grpc::InsecureChannelCredentials());
                    auto stub = Shardkv::NewStub(channel);
                    
                    std::string user_post_key = user_req + "_posts";
                    
                    int i = 0;
                    
                    while(i < MAX_TRIAL){
                        
                        ::grpc::ClientContext cc;
                        AppendRequest req;
                        Empty res;
                        
                        req.set_key(user_post_key);
                        req.set_data(key_req);
                        
                        auto stat = stub->Append(&cc, req, &res);
                        if(stat.ok())
                        
                            break;
                        
                        else{
                        
                            std::this_thread::sleep_for(timespan);
                            i++;
                        
                        }
                    }
                    if (i == MAX_TRIAL){
                        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not possible to contact the right server!");
                    }
                }
            }
            
            this->post_usr[key_req] = user_req;
        }
        else{
            this->database["all_users"] += (key_req+",");
        }
    }
    this->database[key_req] = data_req;
    return ::grpc::Status::OK;
}


::grpc::Status ShardkvServer::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {

    

    
    if(this->primary_address == this->address){

        if(!this->backup_address.empty()){

            auto channel = ::grpc::CreateChannel(this->backup_address, ::grpc::InsecureChannelCredentials());
            auto kvStub = Shardkv::NewStub(channel);
            ::grpc::ClientContext cc;
            
            auto status = kvStub->Append(&cc, *request, response);

            if (status.ok()){
            std::cout << "SUCCESS" << std::endl;
            }else{


                return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Shardmanager FAILED");
            }
        }
    }

    std::string key = request->key();
    std::string data = request->data();
    
    int key_id;
    std::string user="";

    if(key[0] == 'p')
        user = post_usr[key];
    else if(key[key.length()-1] == 's'){
        this->database[key] += (data+",");
        return ::grpc::Status::OK;
    }
    else
        user = key;
    
    key_id = extractID(key);
    
    if(this->key_server.find(key_id) == this->key_server.end() || this->key_server[key_id] != this->shardmanager_address){
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not responsible for the Key!");
    }


    if(this->database.find(key) == this->database.end()){

        if(key[0] == 'p'){
            if(user != "")
                this->database[user+"_posts"] += (key+",");
        }
        else
            this->database["all_users"] += (key+",");
        this->database[key] = data;
    }
    else
        this->database[key] += data;
    
    return ::grpc::Status::OK;
}




::grpc::Status ShardkvServer::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    std::string key = request->key();
    

    
    if(this->database.find(key)!=this->database.end())
        this->database.erase(key);
    else{

        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not responsible for the Key!");
    }
    if(key[0]=='p'){
        if(post_usr[key]!=""){
            int uid = extractID(post_usr[key]);
            if(key_server[uid] == this->shardmanager_address){
                std::string userp = post_usr[key] + "_posts";
                std::string lis = database[userp];
                std::vector<std::string> str_v = parse_value(lis, ",");
                str_v.erase(find(str_v.begin(), str_v.end(), key));
                lis = "";
                for(auto s:str_v){
                    lis += s;
                    lis += ",";
                }
                database[userp] = lis;
            }
        }
        post_usr.erase(key);
    }
    else{
        std::string lis = database["all_users"];
        std::vector<std::string> str_v = parse_value(lis, ",");
        str_v.erase(find(str_v.begin(), str_v.end(), key));
        lis = "";
        for(auto s:str_v){
            lis += s;
            lis += ",";
        }
        database["all_users"] = lis;
    }

    return ::grpc::Status::OK;
}

void ShardkvServer::QueryShardmaster(Shardmaster::Stub* stub) {
    Empty query;
    QueryResponse response;
    ::grpc::ClientContext cc;
    std::chrono::milliseconds timespan(100);


    auto status = stub->Query(&cc, query, &response);

    if(status.ok()){
        
        for(int i = 0; i < response.config_size(); i++){
            
            std::string serv = response.config(i).server();
            
            for(int j = 0; j < response.config(i).shards_size(); j++){
                
                int low = response.config(i).shards(j).lower();
                int up = response.config(i).shards(j).upper();
                
                for(int k = low; k <= up; k++){
                    

                    if(key_server.find(k) != key_server.end()){


                        if((serv != key_server[k]) && (key_server[k] == this->shardmanager_address)){
                            
                            auto channel = grpc::CreateChannel(serv, grpc::InsecureChannelCredentials());
                            auto stub = Shardkv::NewStub(channel);

                            std::string usr = "user_" + std::to_string(k);
                            
                            if(database.find(usr) != database.end()){

                                int i = 0;

                                while(i<MAX_TRIAL){
                                    
                                    ::grpc::ClientContext cc;
                                    PutRequest req;
                                    Empty res;
                                    
                                    req.set_key(usr);
                                    req.set_user(usr);
                                    req.set_data(database[usr]);
                                    
                                    auto stat = stub->Put(&cc, req, &res);

                                    if(stat.ok()){
                                        
                                        this->database.erase(usr);
                                        
                                        std::string lis = database["all_users"];
                                        std::vector<std::string> str_v = parse_value(lis, ",");
                                        
                                        str_v.erase(find(str_v.begin(), str_v.end(), usr));
                                        
                                        lis="";
                                        
                                        for(auto s:str_v){
                                            
                                            lis += s;
                                            lis += ",";
                                        }

                                        database["all_users"] = lis;
                                        break;
                                    }
                                    else{
                                        i++;
                                        std::this_thread::sleep_for(timespan);
                                    }
                                }
                                if (i == MAX_TRIAL){

                                    std::cout << "SERVER OUT OF SERVICE" << std::endl;
                                    exit(1);
                                }
                            }

                            std::string pst = "post_" + std::to_string(k);

                            if(database.find(pst) != database.end()){

                                int i = 0;

                                while(i<MAX_TRIAL){

                                    ::grpc::ClientContext cc;
                                    PutRequest req;
                                    Empty res;
                                    
                                    req.set_key(pst);
                                    req.set_data(database[pst]);
                                    
                                    auto stat = stub->Put(&cc, req, &res);

                                    if(stat.ok()){

                                        this->database.erase(pst);
                                        this->post_usr.erase(pst);
                                        break;

                                    }
                                    else{

                                        i++;
                                        std::this_thread::sleep_for(timespan);
                                    }
                                }
                                if (i == MAX_TRIAL){

                                    std::cout << "SERVER OUT OF SERVICE" << std::endl;
                                    exit(1);
                                }
                            }
                            std::string uip = usr + "_posts";
                            if(database.find(uip) != database.end()){

                                int i = 0;

                                while(i < MAX_TRIAL){
                                    ::grpc::ClientContext cc;
                                    PutRequest req;
                                    Empty res;

                                    req.set_key(uip);
                                    req.set_data(database[uip]);
                                    auto stat = stub->Put(&cc, req, &res);
                                    if(stat.ok()){
                                        this->database.erase(uip);    
                                        break;                                
                                    }
                                    else{
                                        i++;
                                        std::this_thread::sleep_for(timespan);
                                    }
                                }
                                if (i == MAX_TRIAL){

                                    std::cout << "SERVER OUT OF SERVICE" << std::endl;
                                    exit(1);
                                }
                            }
                        }
                    }


                    this->key_server[k] = serv;
                }
            }
        }

    }
    else{

        exit(1);
    }
}



void ShardkvServer::PingShardmanager(Shardkv::Stub* stub) {
    
    std::unique_lock<std::mutex> lock(this->skv_mtx);

    PingRequest request;
    PingResponse response;
    ::grpc::ClientContext cc;

    request.set_server(this->address);
    request.set_viewnumber(this->viewnumber);

    auto stat = stub->Ping(&cc, request, &response);

    this->viewnumber = response.id();
    this->backup_address = response.backup();
    this->primary_address = response.primary();

    if(stat.ok()){
        if (shardmaster_address.empty()){
            std::string response_address = response.shardmaster();
            this->shardmaster_address.assign(response_address);

            std::cout << "PRIMARY " << this->primary_address << " BACKUP " << this->backup_address << std::endl;
            if (this->primary_address != this->address){
                
                auto channel = grpc::CreateChannel(this->primary_address, grpc::InsecureChannelCredentials());
                auto stub = Shardkv::NewStub(channel);
                ::grpc::ClientContext cc;
                DumpResponse response;
                Empty request;

                auto stat = stub->Dump(&cc, request, &response);
                if (stat.ok()){
                    
                    for( const auto& kv : response.database() )
                        this->database.insert({kv.first, kv.second});

                    std::cout << "SUCCESS" << std::endl;
                } else{
                    std::cout << "FAILURE" << std::endl;
                }
            }

        }
    }
    else{
        lock.unlock();
      
        return;
    }

    

    lock.unlock();

    return; 
}



::grpc::Status ShardkvServer::Dump(::grpc::ServerContext* context, const Empty* request, ::DumpResponse* response) {

    std::unique_lock<std::mutex> lock(this->skv_mtx);
    auto dataset = response->mutable_database();
    
    for( const auto& kv : this->database ){
        std::cout << "COPYING " << kv.first << " " << kv.second << std::endl;
        dataset->insert({kv.first, kv.second});
    }

    lock.unlock();
    return ::grpc::Status::OK;
}