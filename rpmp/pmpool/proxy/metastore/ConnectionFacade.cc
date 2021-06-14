#include "ConnectionFacade.h"

#include <string>
#include <iostream>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "redis/RRedis.h"

using namespace std;
using namespace ROCKSDB_NAMESPACE;

ConnectionFacade::ConnectionFacade(std::shared_ptr<Config> config, std::shared_ptr<RLog> log,string type){
  config_ = config;
  log_ = log;
  type_ = type;
}

//RocksDB
int ConnectionFacade::connect(string DBPath){
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, DBPath, &db_);

  if (s.ok() == true){
    setConnected(true);
    return 0;
  }
  setConnected(false);
  return -1;
}

string ConnectionFacade::put(string key, string value){
  if(type_ == ROCKS){
    Status s = db_->Put(WriteOptions(), key, value);
    return s.ToString();
  }else{
    /**
    string cmd = "set " + key + " " + value;
    return send_str(cmd);
    **/
    return rredis_->set(key, value);
  }
}

string ConnectionFacade::get(string key){
  if(type_ == ROCKS){
    string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    return value;
  }else{
    /**
    string cmd = "get " + key;
    return send_str(cmd);
    **/
    return rredis_->get(key);
  }
}

int ConnectionFacade::exists(string key){
  if(type_ == ROCKS){
    string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    if(s.ok()){
      return 1;
    }
    return 0;
  }else{
    /**
    string cmd = "exists " + key;
    return send_int(cmd);
    **/
    return rredis_->exists(key);
  }
}

//Redis
int ConnectionFacade::connect(){
  /**
  context_ = shared_ptr<redisContext>(redisConnect(ip.c_str(), stoi(port)));
  if (context_->err == 0){
    setConnected(true);
    return 0;
  }
  setConnected(false);
  **/
  rredis_ = make_shared<RRedis>(config_, log_);
  rredis_ -> connect();
  setConnected(true);
  return 0;
}

string ConnectionFacade::send_str(string cmd){
  /**
  redisReply *reply = (redisReply*)redisCommand(context_.get(), cmd.c_str());
  string response = reply->str;
  freeReplyObject(reply);
  return response;
  **/
  return "";
}

int ConnectionFacade::send_int(string cmd){
  /**
  redisReply *reply = (redisReply*)redisCommand(context_.get(), cmd.c_str());
  int response = reply->integer;
  freeReplyObject(reply);
  return response;
  **/
  return 0;
}

//Common
bool ConnectionFacade::isConnected(){
  return connected_;
}

int ConnectionFacade::setConnected(bool connected){
  connected_ = connected;
  return 0;
}