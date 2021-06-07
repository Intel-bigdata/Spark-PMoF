#ifndef SPARK_PMOF_REDIS_H
#define SPARK_PMOF_REDIS_H

#include <memory>

#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "Connection.h"

class Config;

class Redis: public std::enable_shared_from_this<Redis>{
public:
  Redis(std::shared_ptr<Config> config, std::shared_ptr <RLog> log_);
  bool connect();
  int connect(string ip, string port);
  string set(string key, string value);   
  string get(string key);
  int hset(string key, map<string, string> *subkeyToSubvalue);
  string hget(string key, string subkey);
  int lpush(string list, string value);
  string lpop(string list);
  string lrange(string list, int start, int end);
  int rpush(string list, string value);
  string rpop(string list);
  string rrange(string list, int start, int end);
  int exists(string key);

private:
  std::shared_ptr<MetastoreConnection> metastoreConnection_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  std::string address_;
  std::string port_;
};

#endif