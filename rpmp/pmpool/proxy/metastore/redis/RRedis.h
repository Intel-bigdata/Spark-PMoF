#ifndef SPARK_PMOF_RREDIS_H
#define SPARK_PMOF_RREDIS_H

#include <memory>
#include "pmpool/Config.h"
#include "pmpool/RLog.h"

#include <sw/redis++/redis++.h>

class Config;

class RRedis: public std::enable_shared_from_this<RRedis>{
public:
  RRedis(std::shared_ptr<Config> config, std::shared_ptr <RLog> log_);
  bool connect();
  string set(string key, string value);   
  string get(string key);
  int exists(string key);

private:
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  std::string address_;
  std::string port_;
  sw::redis::Redis* redis_;
};

#endif