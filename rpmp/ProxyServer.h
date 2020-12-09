#ifndef RPMP_PROXYSERVER_H
#define RPMP_PROXYSERVER_H

#include "pmpool/proxy/ConsistentHash.h"
#include "pmpool/ProxyEvent.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Config.h"
#include "pmpool/Log.h"

using moodycamel::BlockingConcurrentQueue;

class ProxyServer;

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
    bool launchServer();
    void enqueue_recv_msg(std::shared_ptr<ProxyRequest> request);
    void handle_recv_msg(std::shared_ptr<ProxyRequest> request);
    private:
    std::shared_ptr<Worker> worker;
    ChunkMgr* chunkMgr;
    std::shared_ptr<Config> config_;
    std::shared_ptr<Log> log_;
};

#endif //RPMP_PROXYSERVER_H
