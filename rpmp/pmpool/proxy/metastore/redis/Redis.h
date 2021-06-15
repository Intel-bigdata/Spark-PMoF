#ifndef SPARK_PMOF_RREDIS_H
#define SPARK_PMOF_RREDIS_H

#include <memory>
#include "pmpool/Config.h"
#include "pmpool/RLog.h"

#include <sw/redis++/redis++.h>
#include <unordered_set>

class Config;

class Redis: public std::enable_shared_from_this<Redis>{
public:
  Redis(std::shared_ptr<Config> config, std::shared_ptr <RLog> log_);
  bool connect();
  string set(string key, string value);   
  string get(string key);
  bool exists(string key);
  std::unordered_set<std::string> scanAll();
  std::unordered_set<std::string> scan(std::string pattern);
  

private:
  bool first = true;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  std::string address_;
  std::string port_;
  sw::redis::Redis* redis_;
};

#endif