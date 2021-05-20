#include "Connection.h"
#include <string>
#include <iostream>
#include <memory>

using namespace std;

int MetastoreConnection::connect(string ip, string port){
  context_ = shared_ptr<redisContext>(redisConnect(ip.c_str(), stoi(port)));
  if (context_->err == 0){
    setConnected(true);
    return 0;
  }
  setConnected(false);
  return -1;
}

bool MetastoreConnection::isConnected(){
  return connected_;
}

int MetastoreConnection::setConnected(bool connected){
  connected_ = connected;
  return 0;
}

string MetastoreConnection::send_str(string cmd){
  redisReply *reply = (redisReply*)redisCommand(context_.get(), cmd.c_str());
  string response = reply->str;
  freeReplyObject(reply);
  return response;
}

int MetastoreConnection::send_int(string cmd){
  redisReply *reply = (redisReply*)redisCommand(context_.get(), cmd.c_str());
  int response = reply->integer;
  freeReplyObject(reply);
  return response;
}



