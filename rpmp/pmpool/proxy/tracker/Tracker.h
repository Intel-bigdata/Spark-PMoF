#ifndef RPMP_PROXY_TRACKER_H
#define RPMP_PROXY_TRACKER_H

#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "pmpool/Proxy.h"
#include "pmpool/proxy/metastore/MetastoreFacade.h"

#include "json/json.h"
#include <chrono>
#include <map>

class Tracker;
class ReplicaService;
class ReplicaRequest;

class Tracker : public std::enable_shared_from_this<Tracker> {
public:
  explicit Tracker();
  explicit Tracker(std::shared_ptr<Config> config, std::shared_ptr <RLog> log, std::shared_ptr<Proxy> proxyServer, std::shared_ptr<MetastoreFacade> metastore, std::shared_ptr<ReplicaService> replicaService);
  ~Tracker();
  void scheduleUnfinishedTasks();

private:
  //JOB_STATUS
  const string JOB_STATUS = "JOB_STATUS";
  const string NODES = "NODES";
  const string NODE = "NODE";
  const string STATUS = "STATUS";
  const string VALID = "VALID";
  const string INVALID = "INVALID";
  const string PENDING = "PENDING";
  //NODE_STATUS
  const string NODE_STATUS = "NODE_STATUS";
  const string PORT = "PORT";
  const string HOST = "HOST";
  std::shared_ptr <Config> config_;
  std::shared_ptr <RLog> log_;
  std::shared_ptr <Proxy> proxy_;
  std::shared_ptr <MetastoreFacade> metastore_;
  std::shared_ptr<ReplicaService> replicaService_;
  void printValue(std::string key);
  atomic<uint64_t> rid_ = {0};
  void getUnfinishedTask(std::string key, bool& has, std::string& node, uint64_t& size, std::unordered_set<PhysicalNode, PhysicalNodeHash>& nodes);
  void scheduleTask(uint64_t key, uint64_t size, string node, unordered_set<PhysicalNode, PhysicalNodeHash> nodes);
  std::string getPort(std::string host);
};
#endif //RPMP_TRACKER_H