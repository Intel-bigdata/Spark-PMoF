#include <iostream>
#include "../../../Config.h"

#include "Redis.h"
#include "json/json.h"

#include <unordered_set>
#include <sw/redis++/redis++.h>

// TODO: RPMP proxy process should not be terminated at runtime when cannot connect to Redis for query, etc.
Redis::Redis(std::shared_ptr<Config> config, std::shared_ptr<RLog> log){
  config_ = config;
  log_ = log;
  address_ = config_->get_metastore_redis_ip();
  port_ = config_->get_metastore_redis_port();
}

/**
 * The redis-plus-plus uses lazy connection
 * 
 **/
bool Redis::connect() {
  // Create an Redis object, which is movable but NOT copyable.
  string connection_str = "tcp://" + address_ + ":" + port_;
  redis_ = new sw::redis::Redis(connection_str);
  return true;
}

/**
 * Set key and value pair
 * @param key
 * @param value
 **/
string Redis::set(string key, string value){
  redis_->set(key, value);
  return "";
}

/**
 * Get value with specific key
 * @param key
 * @return value
 **/
string Redis::get(string key){
  auto val = redis_->get(key);    // val is of type OptionalString. See 'API Reference' section for details.
  return val.value();
};

/**
 * Check whether specific key exists
 * @param key
 * True or false
 **/
bool Redis::exists(string key){
  long long res = redis_->exists(key);
  if (res == 1){
    return true;
  }
  return false;
}

/**
 * List through all keys
 * @return Set of keys 
 **/
std::unordered_set<std::string> Redis::scanAll(){
  return scan("*");
}

/**
 * List through all keys obey specific pattern
 * 
 * @param pattern to filter keys
 * @return Set of keys matching the pattern
 *
 **/
std::unordered_set<std::string> Redis::scan(std::string pattern){
  std::unordered_set<std::string> keys;
  auto cursor = 0LL;
  auto count = 1;
  int index = 0;
  while (true) {
    cursor = redis_->scan(cursor, pattern, count, std::inserter(keys, keys.begin()));
    if (cursor == 0) {
        break;
    }
    
  }
  #ifdef DEBUG
  for(const auto& elem: keys){
    index++;
    cout<<"count: "<<to_string(index)<<endl;
    cout<<elem<<endl; 
  }
  #endif
  return keys; 
}
