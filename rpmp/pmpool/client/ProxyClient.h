#ifndef RPMP_CLIENT_H
#define RPMP_CLIENT_H

#include <cstring>

#include "HPNL/Callback.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Client.h"
#include "HPNL/Connection.h"

#include <iostream>
#include <thread>
#include <unistd.h>
#include <mutex>
#include <condition_variable>

class ProxyClient;

#define MSG_SIZE 4096


#define BUFFER_SIZE (65536 * 2)
#define BUFFER_NUM 128

using namespace std;

class ProxyClientShutdownCallback : public Callback {
public:
  explicit ProxyClientShutdownCallback(Client* _client) : client(_client) {}
  ~ProxyClientShutdownCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  Client* client;
};


class ProxyClientConnectedCallback : public Callback {
public:
  explicit ProxyClientConnectedCallback(ChunkMgr* chunkMgr_, ProxyClient* proxyClient_) : chunkMgr(chunkMgr_), proxyClient(proxyClient_) {}
  ~ProxyClientConnectedCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  ChunkMgr* chunkMgr;
  ProxyClient* proxyClient;
};

class ProxyClientRecvCallback : public Callback {
public:
  ProxyClientRecvCallback(Client* client_, ChunkMgr* chunkMgr_, ProxyClient* proxyClient_) : client(client_), chunkMgr(chunkMgr_), proxyClient(proxyClient_) {}
  ~ProxyClientRecvCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  Client* client;
  ChunkMgr* chunkMgr;
  ProxyClient* proxyClient;
};

class ProxyClientSendCallback : public Callback {

public:
  explicit ProxyClientSendCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
  ~ProxyClientSendCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  ChunkMgr* chunkMgr;
};

class ProxyClient{
  public:
    void initProxyClient();
    string getAddress(uint64_t hashValue);
    string getAddress(string key);
    void send(const char* data, uint64_t size);
    void received(char* data);
    void setConnection(Connection* connection);

  private:
    ChunkMgr* chunkMgr_;
    Connection *proxy_connection_;
    std::mutex mtx;
    std::condition_variable cv;
    bool received_ = false;
    string s_data;
};

#endif //RPMP_CLIENT_H
