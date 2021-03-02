#include <iostream>
#include "../../Config.h"

#include "Redis.h"
#include "hiredis.h"

#include "json/json.h"

Redis::Redis(std::shared_ptr<Config> config, std::shared_ptr<Log> log){
  config_ = config;
  log_ = log;
  string ip = config_->get_metastore_redis_ip();
  string port = config_->get_metastore_redis_port();
  metastoreConnection_->connect(ip, port);
}

string Redis::set(string key, string value){
  string cmd = "set " + key + " " + value;
  return metastoreConnection_->send_str(cmd);
}

string Redis::get(string key){
  string cmd = "get " + key;
  return metastoreConnection_->send_str(cmd);
};

int Redis::hset(string key, map<string, string> *subkeyToSubvalue){
  string cmd = "hset " + key + " "; 

  map<string, string>::iterator it;

  for (it = subkeyToSubvalue->begin(); it != subkeyToSubvalue->end(); it++)
  {
    cmd += it->first + " " + it->second + " ";
  }

  return metastoreConnection_->send_int(cmd);
}

string Redis::hget(string key, string subkey){
  string cmd = "hget " + key + " " + subkey;
  return metastoreConnection_->send_str(cmd);
}

int Redis::lpush(string list, string value){
  string cmd = "lpush " + list + " " + value;
  return metastoreConnection_->send_int(cmd);
}

string Redis::lpop(string list){
  string cmd = "lpop " + list;
  return metastoreConnection_->send_str(cmd);
}

string Redis::lrange(string list, int start, int end){
  string cmd = "lrange " + list + " " + to_string(start) + " " + to_string(end);
  return metastoreConnection_->send_str(cmd);
}

int Redis::rpush(string list, string value){
  string cmd = "rpush " + list + " " + value;
  return metastoreConnection_->send_int(cmd);
}

string Redis::rpop(string list){
  string cmd = "rpop " + list;
  return metastoreConnection_->send_str(cmd);
}

string Redis::rrange(string list, int start, int end){
  string cmd = "rrange " + list + " " + to_string(start) + " " + to_string(end);
  return metastoreConnection_->send_str(cmd);
}

int Redis::exists(string key){
  string cmd = "exists " + key;
  return metastoreConnection_->send_int(cmd);
}


