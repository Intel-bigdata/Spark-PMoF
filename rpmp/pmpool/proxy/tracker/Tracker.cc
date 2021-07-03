#include "pmpool/proxy/tracker/Tracker.h"

#include "pmpool/Digest.h"

Tracker::Tracker(){

}

Tracker::Tracker(std::shared_ptr <Config> config, std::shared_ptr<RLog> log, std::shared_ptr<Proxy> proxy, std::shared_ptr<MetastoreFacade> metastore, std::shared_ptr<ReplicaService> replicaService) :
    config_(config), log_(log), proxy_(proxy), metastore_(metastore), replicaService_(replicaService)
{

}

Tracker::~Tracker(){

}

/**
 * Get potential unfinished task corresponding to specific key
 * 
 * @param key 
 * @param has Whether have unfinished task
 * @param node Node that contains valid data
 * @param size The size of the data
 * @param nodes A list of node that doesn't have invalid data, which means the task to replicate data to those nodes haven't completed
 * 
 **/
void Tracker::getUnfinishedTask(std::string key, bool& has, string& node, uint64_t& size, std::unordered_set<PhysicalNode, PhysicalNodeHash>& nodes){

  string rawJson = metastore_->get(key);
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
    std::cout << "Tracker::getUnfinishedTask::Error occurred in UpdateRecord." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex length = recordArray.size(); 
  Json::Value data;
  
  #ifdef DEBUG
  cout<<"key: "<<key<<endl;
  #endif
  for(Json::ArrayIndex i = 0; i < length; i++){
    #ifdef DEBUG
    cout<<"node: "<< recordArray[i][NODE]<<endl;
    cout<<"status: "<< recordArray[i][STATUS]<<endl;
    cout<<"size: "<< recordArray[i][SIZE]<<endl;
    #endif
    if(recordArray[i][STATUS] == PENDING){
      has = true;
      std::string node_temp = recordArray[i][NODE].asString();
      std::string port = getPort(key);
      PhysicalNode physicalNode = {node_temp, port};
      nodes.insert(physicalNode);
    }else{
      node = recordArray[i][NODE].asString();
      std::string s_size = recordArray[i][SIZE].asString();
      size = strtoull(s_size.c_str(), NULL, 0);
    }
  }
}

/**
 * Get port based on host from metastore's NODE_STATUS table
 *
 **/
std::string Tracker::getPort(string host){
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
      if(record[HOST] == host){
        return record[PORT].asString();
      }
  }
  return "";
}

/**
 * Schedule all unfinished task by checking metastore
 * 
 **/
void Tracker::scheduleUnfinishedTasks(){
  map<string, string>  res;
  std::unordered_set<std::string> keys = metastore_->scan("[0-9]*");
  int index = 0;
  for(const auto& key: keys){
    index++;
    #ifdef DEBUG
    cout<<"count: "<<to_string(index)<<endl;
    printValue(key);
    #endif
    bool has = false;
    uint64_t size;
    std::string node;
    unordered_set<PhysicalNode, PhysicalNodeHash> nodes;
    getUnfinishedTask(key, has, node, size, nodes);
    if(has == true){
      uint64_t key_uint = stoull(key);
      scheduleTask(key_uint, size, node, nodes);
    }
  }
}



/**
 * Schedule unfinished task corresponding to specific key
 *
 **/
void Tracker::scheduleTask(uint64_t key, uint64_t size, string node, unordered_set<PhysicalNode, PhysicalNodeHash> nodes){
  #ifdef DEBUG
  for(auto elem: nodes){
    cout<<elem.getIp()<<endl;
  }
  #endif
  auto rrc = ReplicaRequestReplyContext();
  rrc.type = REPLICATE_DIRECT;
  rrc.rid = rid_++;
  rrc.key = key;
  rrc.size = size;
  rrc.nodes = nodes;
  Connection* con = replicaService_->getConnection(node); 
  if(con == nullptr){
    return;
  }
  rrc.con = con;    
  auto reply = std::make_shared<ReplicaRequestReply>(rrc);
  reply->encode();
  auto chunk = replicaService_->getChunkMgr()->get(con);
  memcpy(reinterpret_cast<char *>(chunk->buffer), reply->data_, reply->size_);
  chunk->size = reply->size_;
  con->send(chunk);
}

/**
 * For debug usage
 *  
 **/
void Tracker::printValue(std::string key){
  string rawJson = metastore_->get(key);
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
    std::cout << "Tracker::printValue::Error occurred in UpdateRecord." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size(); 
  Json::Value data;
  
  cout<<"key: "<<key<<endl;
  for(Json::ArrayIndex i = 0; i < size; i++){
    cout<<"node: "<< recordArray[i][NODE];
    cout<<"status: "<< recordArray[i][STATUS];
    cout<<"size: "<< recordArray[i][SIZE];
  }
}