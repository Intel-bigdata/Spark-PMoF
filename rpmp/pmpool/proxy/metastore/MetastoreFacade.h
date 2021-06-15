#ifndef SPARK_PMOF_METASTOREFACADE_H
#define SPARK_PMOF_METASTOREFACADE_H

#include <memory>

#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "ConnectionFacade.h"

class Config;

/**
 * Facade for metastore, either Redis or RocksDB
 * 
 **/
class MetastoreFacade: public std::enable_shared_from_this<MetastoreFacade>{
public:
  MetastoreFacade(std::shared_ptr<Config> configs, std::shared_ptr<RLog> log, std::string type);
  //Common
  bool connect();
  string set(string key, string value);   
  string get(string key);
  int exists(string key);
  std::unordered_set<std::string> scanAll();
  std::unordered_set<std::string> scan(string pattern);
private:
  std::shared_ptr<ConnectionFacade> connection_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  std::string address_;
  std::string port_;
  std::string type_;
  std::string REDIS = "REDIS";
  std::string ROCKS = "ROCKS";
};

#endif