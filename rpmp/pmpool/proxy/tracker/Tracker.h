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
  explicit Tracker(std::shared_ptr<Config> config, std::shared_ptr <RLog> log, std::shared_ptr<Proxy> proxyServer, std::shared_ptr<MetastoreFacade> metastore);
  ~Tracker();
  map<string, string> getUnfinishedTask();
  void scheduleTask(ReplicaRequest request);
  void scheduleTask();

private:
  const string JOB_STATUS = "JOB_STATUS";
  const string NODES = "NODES";
  const string NODE = "NODE";
  const string STATUS = "STATUS";
  const string VALID = "VALID";
  const string INVALID = "INVALID";
  const string PENDING = "PENDING";
  std::shared_ptr <Config> config_;
  std::shared_ptr <RLog> log_;
  std::shared_ptr <Proxy> proxy_;
  std::shared_ptr <MetastoreFacade> metastore_;
  std::shared_ptr<ReplicaService> replicaService_;
  void printValue(std::string key);
};
#endif //RPMP_TRACKER_H