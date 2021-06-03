#ifndef SPARK_PMOF_ROCKSCONNECTION_H
#define SPARK_PMOF_ROCKSCONNECTION_H

#include <memory>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace std;
using namespace ROCKSDB_NAMESPACE;

class RocksConnection: public std::enable_shared_from_this<RocksConnection>{
public:
  int connect(string DBPath);
  bool isConnected();
  Status put(string key, string value);
  string get(string key);
  int exists(string key);
private:
  bool connected_;
  int setConnected(bool connected);
  DB *db_;
};

#endif