#include <cstring>

#include "HPNL/Callback.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Client.h"
#include "HPNL/Connection.h"
#include "pmpool/Digest.h"

#include "Client.h"

#include <iostream>
#include <thread>
#include <unistd.h>

#define MSG_SIZE 4096


#define BUFFER_SIZE (65536 * 2)
#define BUFFER_NUM 128

using namespace std;
Connection *connection_;

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
      connection_ = static_cast<Connection*>(param_1);
      thread t1(MessageSender(), connection_, chunkMgr);
      t1.detach();
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
      cout<<"The node got from server is: "<<(char*)chunk->buffer<<endl;
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
      if(chunk == nullptr){
          return;
      }
      auto connection = static_cast<Connection*>(chunk->con);
      chunkMgr->reclaim(chunk, connection);
    }

  private:
    ChunkMgr* chunkMgr;
};

void MessageSender::operator()(Connection* connection, ChunkMgr* chunkMgr)
{
    auto chunk = chunkMgr->get(connection);
    for (int i = 0; i < 10; ++i) {
        chunk->size = 20;
        string src = "1234567" + to_string(i);

        memcpy(chunk->buffer,src.c_str(), 8);
        connection->send(chunk);
    }
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    cout << "Please specify active proxy address and optionally specify client service port: "
         << "--proxy_addr $addr [--port $port]\n";
    return -1;
  }
  string proxy_addr;
  // Default port is assigned.
  string port = "12350";
  for (int i = 1; i < argc; i++) {
    string arg = argv[i];
    if (arg == "--proxy_addr" && i < argc - 1) {
      proxy_addr = argv[i + 1];
    } else if (arg == "--port" && i < argc - 1) {
      port = argv[i + 1];
    }
  }
  if (proxy_addr == "") {
    cout << "No active proxy address is given!\n";
    return -1;
  }

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
  cout << "Trying to connect to " << proxy_addr << ":" << port << endl;
  // Active proxy address, proxy client service port.
  client->connect(proxy_addr.c_str(), port.c_str());
  cout << "Waiting for active proxy to respond.." << endl;
  client->wait();

  delete shutdownCallback;
  delete connectedCallback;
  delete sendCallback;
  delete recvCallback;
  delete client;
  delete chunkMgr;

  return 0;
}


