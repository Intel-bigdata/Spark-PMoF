#include <iostream>
#include "../../../Config.h"

#include "Rocks.h"
#include "json/json.h"

Rocks::Rocks(std::shared_ptr<Config> config, std::shared_ptr<RLog> log){
  log_ = log;
  config_ = config;
  rocksConnection_ = std::make_shared<RocksConnection>();
}

bool Rocks::connect(string DBPath) {
  int res = rocksConnection_->connect(DBPath);
  if (res == 0) {
    log_->get_console_log()->info("Successfully connected to rocksdb database");
    return true;
  }
  log_->get_console_log()->error("Failed to connect to rocksdb database");
  return false;
}

string Rocks::set(string key, string value){
  rocksConnection_->put(key, value);
  return "";
}

string Rocks::get(string key){
  string value = rocksConnection_->get(key);
  return value;
};

int Rocks::exists(string key){
  return rocksConnection_->exists(key);
}