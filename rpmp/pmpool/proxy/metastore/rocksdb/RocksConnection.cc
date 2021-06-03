#include "RocksConnection.h"

#include <string>
#include <iostream>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace std;
using namespace ROCKSDB_NAMESPACE;

int RocksConnection::connect(string DBPath){
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, DBPath, &db_);

  if (s.ok() == true){
    setConnected(true);
    return 0;
  }
  setConnected(false);
  return -1;
}

bool RocksConnection::isConnected(){
  return connected_;
}

int RocksConnection::setConnected(bool connected){
  connected_ = connected;
  return 0;
}

Status RocksConnection::put(string key, string value){
  Status s = db_->Put(WriteOptions(), key, value);
  return s;
}

string RocksConnection::get(string key){
  string value;
  Status s = db_->Get(ReadOptions(), key, &value);
  return value;
}

int RocksConnection::exists(string key){
  string value;
  Status s = db_->Get(ReadOptions(), key, &value);
  if(s.ok()){
    return 1;
  }
  return 0;
}
