#include "pmpool/proxy/tracker/Tracker.h"

Tracker::Tracker(){

}

Tracker::Tracker(std::shared_ptr <Config> config, std::shared_ptr<RLog> log, std::shared_ptr<Proxy> proxy, std::shared_ptr<MetastoreFacade> metastore) :
    config_(config), log_(log), proxy_(proxy), metastore_(metastore)
{

}

Tracker::~Tracker(){

}

map<string, string> Tracker::getUnfinishedTask(){
  map<string, string>  res;
  std::unordered_set<std::string> keys = metastore_->scan("[0-9]*");
  int index = 0;
  /**
  for(const auto& elem: keys){
    index++;
    cout<<"count: "<<to_string(index)<<endl;
    printValue(elem);
  }
  **/ 
  return res;
}

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
    std::cout << "Error occurred in UpdateRecord." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size(); 
  Json::Value data;
  
  cout<<"key: "<<key<<endl;
  for(Json::ArrayIndex i = 0; i < size; i++){
    cout<<"node: "<< recordArray[i][NODE];
    cout<<"status: "<< recordArray[i][STATUS];
  }
}

void Tracker::scheduleTask(ReplicaRequest request){
  
}

void Tracker::scheduleTask(){
  auto rc = ReplicaRequestContext();
  rc.type = REPLICATE;
}