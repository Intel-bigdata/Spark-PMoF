#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;
using namespace std;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

int put_and_get(){
  DB* db;   
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  cout<<to_string(s.ok())<<endl;
  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value1");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");
  cout<<"value: "<<value<<endl;
  return 0;
}

int main(int argc, char **argv){
  put_and_get();
	return 0;	
}