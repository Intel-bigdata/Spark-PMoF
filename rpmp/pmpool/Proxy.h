#ifndef RPMP_PROXY_H
#define RPMP_PROXY_H

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include "pmpool/HeartbeatClient.h"
#include "pmpool/proxy/ConsistentHash.h"
#include "pmpool/ProxyEvent.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "pmpool/proxy/tracker/Tracker.h"
#include "pmpool/proxy/clientService/ClientService.h"
#include "pmpool/proxy/replicaService/ReplicaService.h"
#include "pmpool/proxy/metastore/MetastoreFacade.h"

using moodycamel::BlockingConcurrentQueue;

class Proxy;
class Tracker;

class Proxy : public std::enable_shared_from_this<Proxy>{
public:
    explicit Proxy(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, std::string currentHostAddr);
    ~Proxy();
    bool launchServer();
    bool isActiveProxy(string currentHostAddr);
    bool launchActiveService();
    bool launchStandbyService();
    bool shouldBecomeActiveProxy();
    std::string getLastActiveProxy();
    std::shared_ptr<HeartbeatClient> getHeartbeatClient();
    int build_connection_with_new_active_proxy();
    void stopStandbyService();
    void wait();
    void enqueue_recv_msg(std::shared_ptr<ProxyRequest> request);
    void handle_recv_msg(std::shared_ptr<ProxyRequest> request);
    void addNode(PhysicalNode physicalNode);
    std::unordered_set<PhysicalNode, PhysicalNodeHash> getNodes(uint64_t key);
    uint32_t getNodeNum();
    void addReplica(uint64_t key, PhysicalNode node);
    std::unordered_set<PhysicalNode, PhysicalNodeHash> getReplica(uint64_t key);
    void removeReplica(uint64_t key);
    void notifyClient(uint64_t key);
    private:
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<Config> config_;
    std::string currentHostAddr_;
    std::shared_ptr<RLog> log_;
    std::shared_ptr<MetastoreFacade> metastore_;
    std::shared_ptr<ConsistentHash> consistentHash_;
    uint32_t loadBalanceFactor_;
    std::shared_ptr<Server> server_;
    std::string dataServerPort_;
    uint32_t dataReplica_;
    std::shared_ptr<ClientService> clientService_;
    std::shared_ptr<ReplicaService> replicaService_;
    std::shared_ptr<Tracker> tracker_;
    std::unordered_map<uint64_t, std::unordered_set<PhysicalNode, PhysicalNodeHash>> replicaMap_;
    std::mutex replica_mtx;
    // Standby service
    std::shared_ptr<HeartbeatClient> heartbeatClient_;
};

/**
 * A callback to take action when the built connection is shut down.
 */
class ActiveProxyShutdownCallback : public Callback {
public:
    explicit ActiveProxyShutdownCallback(std::shared_ptr<Proxy> proxy);
    ~ActiveProxyShutdownCallback() override = default;
    void operator()(void* param_1, void* param_2);

private:
    std::shared_ptr<Proxy> proxy_;
    int heartbeatTimeoutInSec_;
};

#endif //RPMP_PROXY_H
