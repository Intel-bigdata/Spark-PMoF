#include <iostream>
#include "../../Config.h"

#include "MetastoreFacade.h"
#include "rocksdb/Rocks.h"
#include "redis/Redis.h"
#include "json/json.h"

MetastoreFacade::MetastoreFacade(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, string type){
  log_ = log;
  config_ = config;
  type_ = type;
  address_ = config_->get_metastore_redis_ip();
  port_ = config_->get_metastore_redis_port();
  connection_ = std::make_shared<ConnectionFacade>(config, log, type);
}

bool MetastoreFacade::connect() {
  int res = 0;
  if(type_ == ROCKS){
    string DBPath = "/tmp/rocksdb_simple_example";
    res = connection_->connect(DBPath);
  }else if(type_ == REDIS){
    res = connection_->connect();
  }
  if (res == 0) {
    log_->get_console_log()->info("Successfully connected to metastore database");
    return true;
  }
  log_->get_console_log()->error("Failed to connect to metastore database");
  return false;
}

string MetastoreFacade::set(string key, string value){
  connection_->put(key, value);
  return "";
}

string MetastoreFacade::get(string key){
  string value = connection_->get(key);
  return value;
}

int MetastoreFacade::exists(string key){
  return connection_->exists(key);
}

std::unordered_set<std::string> MetastoreFacade::scanAll(){
  return connection_->scanAll();
}


std::unordered_set<std::string> MetastoreFacade::scan(std::string pattern){
  return connection_->scan(pattern);
}
