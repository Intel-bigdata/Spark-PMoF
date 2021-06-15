#ifndef SPARK_PMOF_CONNECTIONFACADE_H
#define SPARK_PMOF_CONNECTIONFACADE_H

#include <memory>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "redis/Redis.h"

#include "pmpool/Config.h"
#include "pmpool/RLog.h"

using namespace std;
using namespace ROCKSDB_NAMESPACE;

/**
 * Facade for connection to Redis and RocksDB
 * 
 **/
class ConnectionFacade: public std::enable_shared_from_this<ConnectionFacade>{
public:
  // RocksDB
  ConnectionFacade(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, string type);
  // Redis 
  int connect();
  // Common
  string put(string key, string value);
  string get(string key);
  int exists(string key);
  std::unordered_set<std::string> scan(string pattern);
  std::unordered_set<std::string> scanAll();
  int connect(string DBPath);
  bool isConnected();

private:
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  bool connected_;
  int setConnected(bool connected);
  string type_;
  string ROCKS = "ROCKS";
  string REDIS = "REDIS";
  // RocksDB
  DB *db_;
  // Redis
  shared_ptr<Redis> redis_;
};

#endif