#include <string.h>
#include <atomic>
#include <thread>  // NOLINT
#include "pmpool/Base.h"
#include "pmpool/Config.h"
#include "pmpool/client/PmPoolClient.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

std::atomic<uint64_t> counter = {0};
std::mutex mtx;
char str[1048576];
std::vector<std::shared_ptr<PmPoolClient>> clients;
std::map<int, std::vector<uint64_t>> addresses;

void func1(int i) {
  while (true) {
    auto count_ = counter++;
    if (count_ < 20480) {
      clients[i]->begin_tx();
      if (addresses.count(i) != 0) {
        auto vec = addresses[i];
        vec.push_back(clients[i]->write(str, 1048576));
      } else {
        std::vector<uint64_t> vec;
        vec.push_back(clients[i]->write(str, 1048576));
        addresses[i] = vec;
      }
      clients[i]->end_tx();
    } else {
      break;
    }
  }
}

int main(int argc, char **argv) {
  /// initialize Config class
  std::shared_ptr<Config> config = std::make_shared<Config>();
  CHK_ERR("config init", config->init(argc, argv));
  std::vector<std::shared_ptr<std::thread>> threads;
  memset(str, '0', 1048576);

  int num = 0;
  std::cout << "start write." << std::endl;
  num = 0;
  counter = 0;
  for (int i = 0; i < 4; i++) {
    auto client =
        std::make_shared<PmPoolClient>(config->get_proxy_addrs(), config->get_proxy_port());
    client->begin_tx();
    client->init();
    client->end_tx();
    clients.push_back(client);
    num++;
  }
  uint64_t start = timestamp_now();
  for (int i = 0; i < num; i++) {
    auto t = std::make_shared<std::thread>(func1, i);
    threads.push_back(t);
  }
  for (int i = 0; i < num; i++) {
    threads[i]->join();
  }
  uint64_t end = timestamp_now();
  std::cout << "pmemkv put test: 1048576 "
            << " bytes test, consumes " << (end - start) / 1000.0
            << "s, throughput is " << 20480 / ((end - start) / 1000.0) << "MB/s"
            << std::endl;
  for (int i = 0; i < num; i++) {
    auto vec = addresses[i];
    while (!vec.empty()) {
      auto address = vec.back();
      vec.pop_back();
      clients[i]->free(address);
    }
  }
  std::cout << "freed." << std::endl;
  for (int i = 0; i < num; i++) {
    clients[i]->wait();
  }
  return 0;
}
