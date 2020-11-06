#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>

using namespace std;

#define MSG_SIZE 4096

class ShutdownCallback:public Callback{
  public:
    ShutdownCallback() = default;
    ~ShutdownCallback() override = default;

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
      chunk->size = MSG_SIZE;
      cout<<(char*)chunk->buffer<<endl;
      //connection->send(chunk);
    }

  private:
    ChunkMgr* chunkMgr;
};

class SendCallback : public Callback {
  public:
    explicit SendCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
    ~SendCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      cout<<"ProxyServer::SendCallback::operator"<<endl;
      int mid = *static_cast<int*>(param_1);
      Chunk* chunk = chunkMgr->get(mid);
      auto connection = static_cast<Connection*>(chunk->con);
      chunkMgr->reclaim(chunk, connection);
    }

  private:
    ChunkMgr* chunkMgr;
};

int main(int argc, char* argv[]){
  int worker_number = 1;
  int initial_buffer_number = 16; 

  int buffer_size = 65536;
  int buffer_number = 128;
  auto server = new Server(worker_number, initial_buffer_number);
  int res = server->init();
  cout << res << endl;

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
  server->listen("172.168.0.209", "12346");
  cout<<"before wait..."<<endl;
  server->wait();
  cout<<"after wait..."<<endl;
  return 0;
}
