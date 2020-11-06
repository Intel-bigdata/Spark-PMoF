#include <cstring>

#include "HPNL/Callback.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Client.h"
#include "HPNL/Connection.h"

#include <iostream>

#define MSG_SIZE 4096


#define BUFFER_SIZE (65536 * 2)
#define BUFFER_NUM 128

using namespace std;

int counter = 0;
uint64_t start_time, end_time = 0;
std::mutex mtx;

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
    std::chrono::milliseconds(1);
}

class ShutdownCallback : public Callback {
  public:
    explicit ShutdownCallback(Client* _client) : client(_client) {}
    ~ShutdownCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      cout<<"Client::ShutdownCallback::operator"<<endl;
      client->shutdown();
    }

  private:
    Client* client;
};

class ConnectedCallback : public Callback {
  public:
    explicit ConnectedCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
    ~ConnectedCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      cout<<"Client::ConnectedCallback::operator"<<endl;
      auto connection = static_cast<Connection*>(param_1);
      Chunk* chunk = chunkMgr->get(connection);
      chunk->size = MSG_SIZE;
      memset(chunk->buffer, '1', MSG_SIZE);
      connection->send(chunk);
    }

  private:
    ChunkMgr* chunkMgr;
};

class RecvCallback : public Callback {
  public:
    RecvCallback(Client* client_, ChunkMgr* chunkMgr_) : client(client_), chunkMgr(chunkMgr_) {}
    ~RecvCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      cout<<"Client::RecvCallback::operator"<<endl;
      int mid = *static_cast<int*>(param_1);
      Chunk* chunk = chunkMgr->get(mid);
      auto connection = static_cast<Connection*>(chunk->con);
      chunk->size = MSG_SIZE;
      //connection->send(chunk);
    }

  private:
    Client* client;
    ChunkMgr* chunkMgr;
};

class SendCallback : public Callback {

  public:
    explicit SendCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
    ~SendCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      cout<<"Client::SendCallback::operator"<<endl;
      int mid = *static_cast<int*>(param_1);
      Chunk* chunk = chunkMgr->get(mid);
      auto connection = static_cast<Connection*>(chunk->con);
      chunkMgr->reclaim(chunk, connection);
    }

  private:
    ChunkMgr* chunkMgr;
};

int main(int argc, char* argv[]){
  auto client = new Client(1, 16);
  client->init();

  int buffer_size = 65536;
  int buffer_number = 128;
  ChunkMgr* chunkMgr = new ChunkPool(client, buffer_size, buffer_number);
  client->set_chunk_mgr(chunkMgr);

  auto recvCallback = new RecvCallback(client, chunkMgr);
  auto sendCallback = new SendCallback(chunkMgr);
  auto connectedCallback = new ConnectedCallback(chunkMgr);
  auto shutdownCallback = new ShutdownCallback(client);

  client->set_recv_callback(recvCallback);
  client->set_send_callback(sendCallback);
  client->set_connected_callback(connectedCallback);
  client->set_shutdown_callback(shutdownCallback);

  client->start();
  client->connect("172.168.0.209", "12346");
  cout<<"Client::wait"<<endl;
  client->wait();
  cout<<"Client::after wait"<<endl;

  delete shutdownCallback;
  delete connectedCallback;
  delete sendCallback;
  delete recvCallback;
  delete client;
  delete chunkMgr;

  return 0;
}


