#ifndef SPARK_PMOF_METASTORECONNECTION_H
#define SPARK_PMOF_METASTORECONNECTION_H

#include <memory>
#include <string>
#include "hiredis.h"

using namespace std;

class MetastoreConnection: public std::enable_shared_from_this<MetastoreConnection>{
public:
  int connect(string ip, string port);
  bool isConnected();
  string send_str(string cmd);
  int send_int(string cmd);
private:
  bool connected_;
  shared_ptr<redisContext> context_;
  int setConnected(bool connected);
};

#endif