#ifndef RPMP_PROXYSERVER_H
#define RPMP_PROXYSERVER_H

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include "pmpool/proxy/ConsistentHash.h"
#include "pmpool/ProxyEvent.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Config.h"
#include "pmpool/Log.h"

using moodycamel::BlockingConcurrentQueue;

class ProxyServer;

class ProxyRecvCallback : public Callback {
public:
    ProxyRecvCallback() = delete;
    explicit ProxyRecvCallback(std::shared_ptr<ProxyServer> proxyServer, std::shared_ptr<ChunkMgr> chunkMgr);
    ~ProxyRecvCallback() override = default;
    void operator()(void* param_1, void* param_2) override;

private:
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<ProxyServer> proxyServer_;
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

    /**
     * Currently, nothing to be done when proxy is shutdown
     */
    void operator()(void* param_1, void* param_2) override{
      cout<<"ProxyServer::ShutdownCallback::operator"<<endl;
    }
};

class ProxyConnectCallback : public Callback {
  public:
  ProxyConnectCallback() = default;
  void operator()(void* param_1, void* param_2) override {
    cout << "ProxyServer::ConnectCallback::operator" << endl;
  }
};

class Worker : public ThreadWrapper {
    public:
    Worker(std::shared_ptr<ProxyServer> proxyServer);
    int entry() override;
    void abort() override;
    void addTask(std::shared_ptr<ProxyRequest> request);

    private:
    std::shared_ptr<ProxyServer> proxyServer_;
    BlockingConcurrentQueue<std::shared_ptr<ProxyRequest>> pendingRecvRequestQueue_;
};

class ProxyServer : public std::enable_shared_from_this<ProxyServer>{
public:
    explicit ProxyServer(std::shared_ptr<Config> config, std::shared_ptr<Log> log);
    ~ProxyServer();
    bool launchServer();
    void enqueue_recv_msg(std::shared_ptr<ProxyRequest> request);
    void handle_recv_msg(std::shared_ptr<ProxyRequest> request);
    private:
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
};

#endif //RPMP_PROXYSERVER_H
