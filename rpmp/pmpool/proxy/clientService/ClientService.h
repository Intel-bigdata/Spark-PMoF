#ifndef RPMP_PROXYCLIENTSERVICE_H
#define RPMP_PROXYCLIENTSERVICE_H

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>
#include <HPNL/Server.h>

#include <unordered_map>
#include <string>
#include <unordered_set>

#include "pmpool/proxy/ConsistentHash.h"
#include "pmpool/ProxyEvent.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Config.h"
#include "pmpool/Log.h"
#include "pmpool/Proxy.h"

using moodycamel::BlockingConcurrentQueue;

class ClientService;
class Proxy;

class ProxyRecvCallback : public Callback {
public:
    ProxyRecvCallback() = delete;
    explicit ProxyRecvCallback(std::shared_ptr<ClientService> service, std::shared_ptr<ChunkMgr> chunkMgr);
    ~ProxyRecvCallback() override = default;
    void operator()(void* param_1, void* param_2) override;

private:
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<ClientService> service_;
};

class ProxySendCallback : public Callback {
public:
    ProxySendCallback() = delete;
    explicit ProxySendCallback(std::shared_ptr<ChunkMgr> chunkMgr);
    ~ProxySendCallback() override = default;
    void operator()(void* param_1, void* param_2) override;

private:
    std::shared_ptr<ChunkMgr> chunkMgr_;
};

class ProxyShutdownCallback:public Callback{
public:
    ProxyShutdownCallback() = default;
    ~ProxyShutdownCallback() override = default;
    void operator()(void* param_1, void* param_2) override{
      cout<<"clientservice::ShutdownCallback::operator"<<endl;
    }
};

class ProxyConnectCallback : public Callback {
  public:
  ProxyConnectCallback() = default;
  void operator()(void* param_1, void* param_2) override {
    cout << "clientservice::ConnectCallback::operator" << endl;
  }
};

class Worker : public ThreadWrapper {
    public:
    Worker(std::shared_ptr<ClientService> service);
    int entry() override;
    void abort() override;
    void addTask(std::shared_ptr<ProxyRequest> request);

    private:
    std::shared_ptr<ClientService> service_;
    BlockingConcurrentQueue<std::shared_ptr<ProxyRequest>> pendingRecvRequestQueue_;
};

class ClientService : public std::enable_shared_from_this<ClientService>{
public:
    explicit ClientService(std::shared_ptr<Config> config, std::shared_ptr<Log> log, std::shared_ptr<Proxy> proxyServer);
    ~ClientService();
    bool startService();
    void wait();
    void enqueue_recv_msg(std::shared_ptr<ProxyRequest> request);
    void handle_recv_msg(std::shared_ptr<ProxyRequest> request);
    private:
    void enqueue_finalize_msg(std::shared_ptr<ProxyRequestReply> reply);
    void handle_finalize_msg(std::shared_ptr<ProxyRequestReply> reply);
    std::shared_ptr<Worker> worker_;
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<Config> config_;
    std::shared_ptr<Log> log_;
    std::shared_ptr<ConsistentHash> consistentHash_;
    std::shared_ptr<Server> server_;
    std::shared_ptr<ProxyRecvCallback> recvCallback_;
    std::shared_ptr<ProxySendCallback> sendCallback_;
    std::shared_ptr<ProxyConnectCallback> connectCallback_;
    std::shared_ptr<ProxyShutdownCallback> shutdownCallback_;
    std::string dataServerPort_;
    uint32_t dataReplica_;
    std::unordered_map<std::string, Connection*> dataServerConnections_;
    std::unordered_map<uint64_t, std::unordered_set<std::string>> replicaMap_;
    std::mutex replica_mtx;
    std::unordered_map<uint64_t, std::shared_ptr<ProxyRequestReply>> prrcMap_;
    std::mutex prrcMtx;
    std::shared_ptr<Proxy> proxyServer_;
};

#endif //RPMP_PROXYCLIENTSERVICE_H
