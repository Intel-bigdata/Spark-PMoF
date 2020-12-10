
#include "pmpool/client/ProxyClient.h"

void ProxyClientShutdownCallback::operator()(void *param_1, void *param_2) {
  cout<<"PRoxyClient::ProxyClientShutdownCallback::operator"<<endl;
  client->shutdown();
}

void ProxyClientConnectedCallback::operator()(void *param_1, void *param_2) {
  cout<<"ProxyClient::ProxyClientConnectedCallback::operator"<<endl;
  auto connection = static_cast<Connection*>(param_1);
  proxyClient->setConnection(connection);
}

void ProxyClientRecvCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int*>(param_1);
  Chunk* chunk = chunkMgr->get(mid);
  /**
  cout<<"The node got from server is: "<<(char*)chunk->buffer<<endl;
   **/
  proxyClient->received((char*)chunk->buffer);
}

void ProxyClientSendCallback::operator()(void *param_1, void *param_2) {
  /**
  int mid = *static_cast<int*>(param_1);
  Chunk* chunk = chunkMgr->get(mid);
  auto connection = static_cast<Connection*>(chunk->con);
  chunkMgr->reclaim(chunk, connection);
   **/
}

void ProxyClient::setConnection(Connection *connection) {
  this->proxy_connection_ = connection;
}

void ProxyClient::received(char *data){
  unique_lock<mutex> lk(mtx);
  received_ = true;
  string temp(data);
  s_data = temp;
  cv.notify_one();
}

void ProxyClient::send(const char *data, uint64_t size) {
  auto chunk = chunkMgr_->get(this->proxy_connection_);
  //cout<<"ProxyClient::send size="<<to_string(size)<<endl;
  //cout<<"ProxyClient::send data="<<data<<endl;
  std::memcpy(reinterpret_cast<char *>(chunk->buffer), data, size);
  chunk->size = size;
  this->proxy_connection_->send(chunk);
}


string ProxyClient::getAddress(uint64_t hashValue){
  unique_lock<mutex> lk(mtx);
  string s_hashValue = to_string(hashValue);
  this->send(s_hashValue.c_str(), s_hashValue.size());
  while(!received_){
    cv.wait(lk);
  }
  received_ = false;
  return s_data;
  /**
  if(hashValue % 2 == 0){
    return "172.168.0.40";
  }
  return "172.168.0.209";
  **/
}

string ProxyClient::getAddress(string key){
  return "";
}

/**
class ProxyClientLauncher{
  public:
    void operator()(Client* client);
};

void ProxyClientLauncher::operator()(Client* client){
  client->wait();
}
**/



void ProxyClient::initProxyClient() {
  auto client = new Client(1, 16);
  client->init();

  int buffer_size = 65536;
  int buffer_number = 128;
  ChunkMgr *chunkMgr = new ChunkPool(client, buffer_size, buffer_number);
  client->set_chunk_mgr(chunkMgr);
  this->chunkMgr_ = chunkMgr;

  auto recvCallback = new ProxyClientRecvCallback(client, chunkMgr, this);
  auto sendCallback = new ProxyClientSendCallback(chunkMgr);
  auto connectedCallback = new ProxyClientConnectedCallback(chunkMgr, this);
  auto shutdownCallback = new ProxyClientShutdownCallback(client);

  client->set_recv_callback(recvCallback);
  client->set_send_callback(sendCallback);
  client->set_connected_callback(connectedCallback);
  client->set_shutdown_callback(shutdownCallback);

  client->start();
  client->connect("172.168.0.209", "12348");
  cout << "ProxyClient::initProxyClient: wait" << endl;
  client->wait();

  delete shutdownCallback;
  delete connectedCallback;
  delete sendCallback;
  delete recvCallback;
  delete client;
  delete chunkMgr;
}
