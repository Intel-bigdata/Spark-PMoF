#ifndef SPARK_PMOF_ROCKS_H
#define SPARK_PMOF_ROCKS_H

#include <memory>

#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "RocksConnection.h"

class Config;

class Rocks: public std::enable_shared_from_this<Rocks>{
public:
  Rocks(std::shared_ptr<Config> configs, std::shared_ptr<RLog> log);
  bool connect(string DBPath);
  string set(string key, string value);   
  string get(string key);
  int exists(string key);

private:
  std::shared_ptr<RocksConnection> rocksConnection_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  std::string address_;
  std::string port_;
};

#endif