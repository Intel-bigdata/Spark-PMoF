#include "NodeManager.h"

#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
#include "NodeManager.h"
#include <thread>

#include "json/json.h"
#include "pmpool/proxy/metastore/JsonUtil.h"

using std::shared_ptr;
using std::string;
using std::vector;

NodeManagerRecvCallback::NodeManagerRecvCallback(std::shared_ptr<NodeManager> nodeManager,
                                                 std::shared_ptr<ChunkMgr> chunkMgr)
    : nodeManager_(nodeManager), chunkMgr_(chunkMgr) {}

void NodeManagerRecvCallback::operator()(void *param_1, void *param_2)
{
  int mid = *static_cast<int *>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto request = std::make_shared<HeartbeatRequest>(
      reinterpret_cast<char *>(chunk->buffer), chunk->size,
      reinterpret_cast<Connection *>(chunk->con));
  request->decode();
  HeartbeatRequestMsg *requestMsg = (HeartbeatRequestMsg *)(request->getData());
  if (requestMsg->type != 0)
  {
    nodeManager_->enqueue_recv_msg(request);
  }
  else
  {
    std::cout << "[RecvCallback::RecvCallback][" << requestMsg->type
              << "] size is " << chunk->size << std::endl;
    for (int i = 0; i < chunk->size; i++)
    {
      printf("%X ", *(request->getData() + i));
    }
    printf("\n");
  }
  chunkMgr_->reclaim(chunk, static_cast<Connection *>(chunk->con));
}

/**
 * The SendCallback is mainly for the reclaim of chunk
 */
NodeManagerSendCallback::NodeManagerSendCallback(std::shared_ptr<ChunkMgr> chunkMgr)
    : chunkMgr_(chunkMgr) {}

void NodeManagerSendCallback::operator()(void *param_1, void *param_2)
{
  int mid = *static_cast<int *>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto connection = static_cast<Connection *>(chunk->con);
  chunkMgr_->reclaim(chunk, connection);
}

NodeManagerWorker::NodeManagerWorker(std::shared_ptr<NodeManager> nodeManager) : nodeManager_(nodeManager) {}

void NodeManagerWorker::addTask(std::shared_ptr<HeartbeatRequest> request)
{
  pendingRecvRequestQueue_.enqueue(request);
}

void NodeManagerWorker::abort() {}

int NodeManagerWorker::entry()
{
  std::shared_ptr<HeartbeatRequest> request;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res)
  {
    nodeManager_->handle_recv_msg(request);
  }
  return 0;
}

NodeManager::NodeManager(shared_ptr <Config> config, shared_ptr <RLog> log, shared_ptr <Proxy> proxy, shared_ptr <MetastoreFacade> metastore) :
    config_(config), log_(log), proxy_(proxy), metastore_(metastore)
{
  hashToNode_ = new map<uint64_t, string>();
  for (std::string node : config_->get_nodes())
  {
    cout<<"NodeManager::node: "<<node<<endl;
    XXHash *hashFactory = new XXHash();
    uint64_t hashValue = hashFactory->hash(node);
    hashToNode_->insert(std::make_pair(hashValue, node));
  }

  map<uint64_t, string>::iterator it;

  #ifdef DEBUG
  for (it = hashToNode_->begin(); it != hashToNode_->end(); it++)
  {
    std::cout << to_string(it->first) // string (key)
              << ':'
              << it->second // string's value
              << std::endl;
  }
  #endif
}

NodeManager::~NodeManager()
{
  worker_->stop();
  worker_->join();
}

void NodeManager::enqueue_recv_msg(std::shared_ptr<HeartbeatRequest> request)
{
  worker_->addTask(request);
}

int64_t NodeManager::getCurrentTime(){
  chrono::milliseconds ms = chrono::duration_cast<chrono::milliseconds >(
    chrono::system_clock::now().time_since_epoch()
  );
  return ms.count();
}

/**
 * For debug usage
 * 
 **/
void NodeManager::printNodeStatus(){
  string rawJson = metastore_->get(NODE_STATUS);
  const auto rawJsonLength = static_cast<int>(rawJson.length());
  JSONCPP_STRING err;
  Json::Value root;
  
  Json::CharReaderBuilder builder;
  const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (!reader->parse(rawJson.c_str(), rawJson.c_str() + rawJsonLength, &root,
                      &err)) {
#ifdef DEBUG
    std::cout << "Error occurred in printing node status." << std::endl;
#endif
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size();    

  for (Json::ArrayIndex i = 0; i < size; i++){
      Json::Value record = recordArray[i];
      cout<<"host: " << record[HOST];
      cout<<"time: " << record[TIME];
      cout<<"status: " << record[STATUS];
      cout<<"port: " << record[PORT];
  }
}

/**
 * Construct Node Status Table 
 **/
void NodeManager::constructNodeStatus(Json::Value record){
  Json::Value root;
  Json::Value data;
  data[0][HOST] = record[HOST];
  data[0][TIME] = record[TIME];
  data[0][STATUS] = record[STATUS];
  data[0][PORT] = record[PORT];
  root["data"] = data;
  string json_str = rootToString(root);
  #ifdef DEBUG
  cout<<"NodeManager::constructNodeStatus::json_str "<<json_str<<endl;
  #endif
  metastore_->set(NODE_STATUS, json_str);
  nodeConnect(record[HOST].asString(), record[PORT].asString());
}


/**
 * Add a new record if new host is connected or update existed host's status
 **/
void NodeManager::addOrUpdateRecord(Json::Value record){
  int exist = metastore_->exists(NODE_STATUS);
  if (exist == 0){
    constructNodeStatus(record);
  }

  string rawJson = metastore_->get(NODE_STATUS);
  #ifdef DEBUG
  cout<<rawJson<<endl;
  #endif
  const auto rawJsonLength = static_cast<int>(rawJson.length());
  JSONCPP_STRING err;
  Json::Value root;
  
  Json::CharReaderBuilder builder;
  const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (!reader->parse(rawJson.c_str(), rawJson.c_str() + rawJsonLength, &root,
                      &err)) {
    std::cout << "Error occurred in addOrUpdateRecord." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size();    

  if(!hostExists(record[HOST].asString())){
    #ifdef DEBUG
    cout<< "add a new record"<<endl;
    #endif
    Json::Value new_root;
    Json::Value new_data;
    for(Json::ArrayIndex i = 0; i < size; i++){
      new_data[i][HOST] = recordArray[i][HOST];
      new_data[i][TIME] = recordArray[i][TIME];
      new_data[i][STATUS] = recordArray[i][STATUS];
      new_data[i][PORT] = recordArray[i][PORT];
    }

    new_data[size][HOST] = record[HOST];
    new_data[size][TIME] = record[TIME];
    new_data[size][STATUS] = record[STATUS];
    new_data[size][PORT] = record[PORT];
    nodeConnect(record[HOST].asString(), record[PORT].asString());
    new_root["data"] = new_data;
    metastore_->set(NODE_STATUS, rootToString(new_root));
    return;
  }
   
  for (Json::ArrayIndex i = 0; i < size; i++){
    if (recordArray[i][HOST].asString() == record[HOST].asString()){
      if(record[STATUS].asString() == LIVE && recordArray[i][STATUS].asString() == DEAD){
        nodeConnect(record[HOST].asString(), record[PORT].asString());
      }
      recordArray[i][TIME] = record[TIME];
      recordArray[i][STATUS] = record[STATUS];
    }
  }

  root["data"] = recordArray;
  metastore_->set(NODE_STATUS, rootToString(root));
  
}

bool NodeManager::hostExists(string host){
  string rawJson = metastore_->get(NODE_STATUS);
  const auto rawJsonLength = static_cast<int>(rawJson.length());
  JSONCPP_STRING err;
  Json::Value root;
  
  Json::CharReaderBuilder builder;
  const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (!reader->parse(rawJson.c_str(), rawJson.c_str() + rawJsonLength, &root,
                      &err)) {
    std::cout << "Error occurred in checking hostExists." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size();    

  for (Json::ArrayIndex i = 0; i < size; i++){
      Json::Value record = recordArray[i];
      if (host == record[HOST].asString()){
        #ifdef DEBUG
        cout<<"host: "<<host<<" exists"<<endl;
        #endif
        return true;
      }
  }
  return false;
}

void NodeManager::handle_recv_msg(std::shared_ptr<HeartbeatRequest> request)
{
  HeartbeatRequestContext rc = request->get_rc();
  auto rrc = HeartbeatRequestReplyContext();
  rrc.type = rc.type;
  rrc.success = 0;
  rrc.rid = rc.rid;
#ifdef DEBUG
  std::cout << "rid: " << to_string(rc.rid) << std::endl;
  std::cout << "host-hash: " << to_string(rc.host_ip_hash) << std::endl;

  map<uint64_t, string>::iterator it;
  for (it = hashToNode_->begin(); it != hashToNode_->end(); it++)
  {
    std::cout << it->first  
              << ':' 
              << it->second 
              << std::endl;
  }
#endif

  if (hashToNode_->count(rc.host_ip_hash) > 0)
  {
    string host = hashToNode_->at(rc.host_ip_hash);
    map<string, string> subkeyToSubvalue;
    subkeyToSubvalue.insert(pair<string, string>(HOST, host));
    subkeyToSubvalue.insert(pair<string, string>(TIME, to_string(getCurrentTime())));

    Json::Value record;
    record[HOST] = host;
    record[TIME] = to_string(getCurrentTime());
    record[STATUS] = LIVE;
    record[PORT] = rc.port;
    addOrUpdateRecord(record);
  }
  rrc.con = rc.con;
  std::shared_ptr<HeartbeatRequestReply> requestReply = std::make_shared<HeartbeatRequestReply>(rrc);
  requestReply->encode();
  auto ck = chunkMgr_->get(rrc.con);
  #ifdef DEBUG
  std::cout << "ck->buffer" << ck->buffer << std::endl;
  std::cout << "requestReply->size: " << requestReply->size_ << std::endl;
  if (requestReply->data_ == nullptr)
  {
    std::cout << "data is null" << std::endl;
  }
  else
  {
    std::cout << "requestReply->data: " << requestReply->data_ << std::endl;
  }
  #endif
  memcpy(reinterpret_cast<char *>(ck->buffer), requestReply->data_, requestReply->size_);
  ck->size = requestReply->size_;
  rrc.con->send(ck);
}

void NodeManager::nodeDead(string node){
  cout<<"dead node: "<<node<<endl;
}

void NodeManager::nodeConnect(string node, string port){
  PhysicalNode physicalNode = {node, port};
  proxy_->addNode(physicalNode);
}

int NodeManager::checkNode(){
  /**
   * For debug usage
   * metastore_->scan("[0-9]*");
   **/
  int heartbeatInterval = config_->get_heartbeat_interval();
  int gap = 2;
  while(true){
    sleep(heartbeatInterval * gap);
    int exist = metastore_->exists(NODE_STATUS);
    if (exist == 0){
      continue;
    }
    string rawJson = metastore_->get(NODE_STATUS);
    const auto rawJsonLength = static_cast<int>(rawJson.length());
    JSONCPP_STRING err;
    Json::Value root;

    Json::CharReaderBuilder builder;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(rawJson.c_str(), rawJson.c_str() + rawJsonLength, &root, &err)) {
      std::cout << "Error occurred in checking node." << std::endl;
    }

    Json::Value recordArray = root["data"];
    Json::ArrayIndex size = recordArray.size(); 

    int64_t currentTime = getCurrentTime();
    for (Json::ArrayIndex i = 0; i < size; i++){
      Json::Value record = recordArray[i];
      string time_str = record[TIME].asString();
      int64_t time = strtol(time_str.c_str(), NULL, 0);
      if ((currentTime - time) > heartbeatInterval * 1000 * gap){
        if(record[STATUS].asString() == LIVE){
          record[STATUS] = DEAD;
          addOrUpdateRecord(record);
          nodeDead(record[HOST].asString());
        }
      }
    }
  }
  return 0;
}

bool NodeManager::startService()
{
  int worker_number = config_->get_network_worker_num();
  int buffer_number = config_->get_network_buffer_num();
  int buffer_size = config_->get_network_buffer_size();
  server_ = std::make_shared<Server>(worker_number, buffer_number);
  if (server_->init() != 0)
  {
    cout << "HPNL server init failed" << endl;
    return false;
  }
  chunkMgr_ = std::make_shared<ChunkPool>(server_.get(), buffer_size, buffer_number);
  server_->set_chunk_mgr(chunkMgr_.get());

  recvCallback_ = std::make_shared<NodeManagerRecvCallback>(shared_from_this(), chunkMgr_);
  sendCallback_ = std::make_shared<NodeManagerSendCallback>(chunkMgr_);
  shutdownCallback_ = std::make_shared<NodeManagerShutdownCallback>();
  connectCallback_ = std::make_shared<NodeManagerConnectCallback>();

  worker_ = std::make_shared<NodeManagerWorker>(shared_from_this());
  worker_->start();

  server_->set_recv_callback(recvCallback_.get());
  server_->set_send_callback(sendCallback_.get());
  server_->set_connected_callback(connectCallback_.get());
  server_->set_shutdown_callback(shutdownCallback_.get());

  server_->start();
  server_->listen(config_->get_current_proxy_addr().c_str(), config_->get_heartbeat_port().c_str());

  std::thread t_nodeChecker(&NodeManager::checkNode, shared_from_this());
  t_nodeChecker.detach();
  log_->get_console_log()->info("NodeManager server started at {0}:{1}", config_->get_current_proxy_addr(), config_->get_heartbeat_port());
  return true;
}

void NodeManager::wait() {
  server_->wait();
}

/**
 * Check whether all nodes listed in config have conncted
 * 
 **/
bool NodeManager::allConnected(){
  int exist = metastore_->exists(NODE_STATUS);
  if (exist == 0){
    return false;
  }
  vector<string> nodes = config_->get_nodes();
  int configured_size = nodes.size();
  string rawJson = metastore_->get(NODE_STATUS);
  const auto rawJsonLength = static_cast<int>(rawJson.length());
  JSONCPP_STRING err;
  Json::Value root;

  Json::CharReaderBuilder builder;
  const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (!reader->parse(rawJson.c_str(), rawJson.c_str() + rawJsonLength, &root, &err)) {
    std::cout << "Error occurred in checking node." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size(); 

  int heartbeatInterval = config_->get_heartbeat_interval();
  int count = 0;

  for(auto node: nodes){
    int64_t currentTime = getCurrentTime();
    bool found = false;
    for (Json::ArrayIndex i = 0; i < size; i++){
      Json::Value record = recordArray[i];
      if(node == record[HOST].asString()){
        found = true;
        if(record[STATUS].asString() != LIVE){
          return false;
        }
        string time_str = record[TIME].asString();
        int64_t time = strtol(time_str.c_str(), NULL, 0);
        if ((currentTime - time) >  1000 * heartbeatInterval){
          return false;
        }
      }
    }
    if(found == false){
      return false;
    }
    count++;
  }

  
  if (count == configured_size){
    return true;
  }
 
  return false;
}
