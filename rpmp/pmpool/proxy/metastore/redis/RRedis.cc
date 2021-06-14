#include <iostream>
#include "../../../Config.h"

#include "RRedis.h"
#include "json/json.h"

#include <sw/redis++/redis++.h>

// TODO: RPMP proxy process should not be terminated at runtime when cannot connect to Redis for query, etc.
RRedis::RRedis(std::shared_ptr<Config> config, std::shared_ptr<RLog> log){
  config_ = config;
  log_ = log;
  address_ = config_->get_metastore_redis_ip();
  port_ = config_->get_metastore_redis_port();
}

/**
 * The redis-plus-plus uses lazy connection
 * 
 **/
bool RRedis::connect() {
  // Create an Redis object, which is movable but NOT copyable.
  string connection_str = "tcp://" + address_ + ":" + port_;
  redis_ = new sw::redis::Redis(connection_str);
  return true;
}

string RRedis::set(string key, string value){
  redis_->set(key, value);
  return "";
}

string RRedis::get(string key){
  auto val = redis_->get(key);    // val is of type OptionalString. See 'API Reference' section for details.
  return val.value();
};

int RRedis::exists(string key){
  long long res = redis_->exists(key);
  if (res == 1){
    return true;
  }
  return false;
}
