#include "ConnectionFacade.h"

#include <string>
#include <iostream>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "redis/Redis.h"

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
    return redis_->set(key, value);
  }
}

string ConnectionFacade::get(string key){
  if(type_ == ROCKS){
    string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    return value;
  }else{
    return redis_->get(key);
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
    return redis_->exists(key);
  }
}

std::unordered_set<std::string> ConnectionFacade::scanAll(){
  if(type_ == ROCKS){
    //Do nothing
  }else{
    return redis_->scanAll();
  }
}

std::unordered_set<std::string> ConnectionFacade::scan(std::string pattern){
  if(type_ == ROCKS){
    //Do nothing
  }else{
    return redis_->scan(pattern);
  }
}

//Redis
int ConnectionFacade::connect(){
  redis_ = make_shared<Redis>(config_, log_);
  redis_ -> connect();
  setConnected(true);
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