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
#include "pmpool/RLog.h"
#include "pmpool/Proxy.h"
#include "pmpool/proxy/metastore/MetastoreFacade.h"
#include "json/json.h"

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

    void operator()(void *param_1, void *param_2) override {
#ifdef DEBUG
      cout << "Clientservice::ProxyShutdownCallback::operator() is called." << endl;
#endif
    }
};

class ProxyConnectCallback : public Callback {
public:
    ProxyConnectCallback() = default;

    void operator()(void *param_1, void *param_2) override {
#ifdef DEBUG
      cout << "Clientservice::ProxyConnectCallback::operator() is called." << endl;
#endif
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
    explicit ClientService(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, std::shared_ptr<Proxy> proxyServer, std::shared_ptr<MetastoreFacade> metastore);
    ~ClientService();
    bool startService();
    void wait();
    void enqueue_recv_msg(std::shared_ptr<ProxyRequest> request);
    void handle_recv_msg(std::shared_ptr<ProxyRequest> request);
    // notify RPMP client replication response
    void notifyClient(uint64_t key);
    private:
    /**
     * JOB_STATUS: 
     *            NODES: {
     *                    NODE:
     *                    STATUS:
     *                    SIZE:
     *
     **/
    const string JOB_STATUS = "JOB_STATUS";
    const string NODES = "NODES";
    const string NODE = "NODE";
    const string STATUS = "STATUS";
    const string VALID = "VALID";
    const string INVALID = "INVALID";
    const string PENDING = "PENDING";
    const string SIZE = "SIZE";
    void enqueue_finalize_msg(std::shared_ptr<ProxyRequestReply> reply);
    void handle_finalize_msg(std::shared_ptr<ProxyRequestReply> reply);
    // std::vector<std::shared_ptr<Worker>> workers_;
    void constructJobStatus(Json::Value record, uint64_t key);
    void addRecords(uint64_t key, unordered_set<PhysicalNode, PhysicalNodeHash> nodes);

    std::shared_ptr<Worker> worker_;
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<Config> config_;
    std::shared_ptr<RLog> log_;
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
    std::shared_ptr<MetastoreFacade> metastore_;
    int count = 0;

};

#endif
