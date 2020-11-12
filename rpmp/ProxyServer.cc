#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
#include "pmpool/proxy/ConsistentHash.h"
#include "ProxyServer.h"

using namespace std;

ConsistentHash<PhysicalNode> *consistentHash;

class ShutdownCallback:public Callback{
public:
    ShutdownCallback() = default;
    ~ShutdownCallback() override = default;

    /**
     * Currently, nothing to be done when proxy is shutdown
     */
    void operator()(void* param_1, void* param_2) override{
      cout<<"ProxyServer::ShutdownCallback::operator"<<endl;
    }
};

class RecvCallback : public Callback {
public:
    explicit RecvCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
    ~RecvCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      cout<<"ProxyServer::RecvCallback::operator"<<endl;
      int mid = *static_cast<int*>(param_1);
      auto chunk = chunkMgr->get(mid);
      auto connection = static_cast<Connection*>(chunk->con);
      cout<<(char*)chunk->buffer<<endl;
      unsigned long key_long = stoull((char*)chunk->buffer);

      string node = consistentHash->getNode(key_long).getKey();
      cout<<"The node got from consistent_hash is: "<<node<<endl;
      chunk->size = node.length();
      memcpy(chunk->buffer,node.c_str(),chunk->size);
      connection->send(chunk);
    }

private:
    ChunkMgr* chunkMgr;
};

/**
 * The SendCallback is mainly for the reclaim of chunk
 */
class SendCallback : public Callback {
public:
    explicit SendCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
    ~SendCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      int mid = *static_cast<int*>(param_1);
      Chunk* chunk = chunkMgr->get(mid);
      auto connection = static_cast<Connection*>(chunk->con);
      chunkMgr->reclaim(chunk, connection);
    }

private:
    ChunkMgr* chunkMgr;
};

bool ProxyServer::launchServer() {
  /**
   * Set the number of virtual nodes for load balance
   */
  int loadBalanceFactor = 5;
  /**
   * The nodes should be come from config, hard code for temp use
   */
  consistentHash = new ConsistentHash<PhysicalNode>();
  PhysicalNode *physicalNode = new PhysicalNode("172.168.0.40");
  consistentHash->addNode(*physicalNode, loadBalanceFactor);

  PhysicalNode *physicalNode2 = new PhysicalNode("172.168.0.209");
  consistentHash->addNode(*physicalNode2, loadBalanceFactor);

  int worker_number = 1;
  int initial_buffer_number = 16;

  int buffer_size = 65536;
  int buffer_number = 128;
  auto server = new Server(worker_number, initial_buffer_number);
  if(server->init() != 0){
    cout<<"HPNL server init failed"<<endl;
    return false;
  }

  ChunkMgr* chunkMgr = new ChunkPool(server, buffer_size, buffer_number);
  server->set_chunk_mgr(chunkMgr);

  auto recvCallback = new RecvCallback(chunkMgr);
  auto sendCallback = new SendCallback(chunkMgr);
  auto shutdownCallback = new ShutdownCallback();

  server->set_recv_callback(recvCallback);
  server->set_send_callback(sendCallback);
  server->set_connected_callback(nullptr);
  server->set_shutdown_callback(shutdownCallback);

  server->start();
  char* testIP = "192.168.124.12";
  char* IP = "172.168.0.609";
  server->listen(testIP, "12346");
  cout<<"RPMP proxy started"<<endl;
  server->wait();
  return true;
}

int main(int argc, char* argv[]){
  ProxyServer* proxyServer;
  proxyServer->launchServer();
  return 0;
}
