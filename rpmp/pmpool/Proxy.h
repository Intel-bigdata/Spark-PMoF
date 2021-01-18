#ifndef RPMP_PROXY_H
#define RPMP_PROXY_H

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
#include "pmpool/proxy/clientService/ClientService.h"
#include "pmpool/proxy/replicaService/ReplicaService.h"

using moodycamel::BlockingConcurrentQueue;

class Proxy;

class Proxy : public std::enable_shared_from_this<Proxy>{
public:
    explicit Proxy(std::shared_ptr<Config> config, std::shared_ptr<Log> log);
    ~Proxy();
    bool launchServer();
    void wait();
    void enqueue_recv_msg(std::shared_ptr<ProxyRequest> request);
    void handle_recv_msg(std::shared_ptr<ProxyRequest> request);
    void addNode(PhysicalNode* physicalNode);
    std::vector<PhysicalNode> getNodes(uint64_t key);
    uint32_t getNodeNum();
    void addReplica(uint64_t key, std::string node, std::string port);
    std::unordered_set<std::string> getReplica(uint64_t key);
    void removeReplica(uint64_t key);
    void notifyClient(uint64_t key);
    private:
    std::shared_ptr<ChunkMgr> chunkMgr_;
    std::shared_ptr<Config> config_;
    std::shared_ptr<Log> log_;
    std::shared_ptr<ConsistentHash> consistentHash_;
    int loadBalanceFactor_;
    std::shared_ptr<Server> server_;
    std::string dataServerPort_;
    uint32_t dataReplica_;
    std::shared_ptr<ClientService> clientService_;
    std::shared_ptr<ReplicaService> replicaService_;
    std::unordered_map<uint64_t, std::unordered_set<std::string>> replicaMap_;
    std::mutex replica_mtx;
};

#endif //RPMP_PROXY_H
