#include <string.h>
#include <cstdlib>
#include <thread>  // NOLINT
#include "pmpool/Base.h"
#include "pmpool/Config.h"
#include "pmpool/client/PmPoolClient.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

int counter = 0;
std::mutex mtx;
std::vector<std::string> keys;
char str[1048576];
int numReqs = 2048;

void func1(std::shared_ptr<PmPoolClient> client) {
  while (true) {
    std::unique_lock<std::mutex> lk(mtx);
    uint64_t count_ = counter++;
    lk.unlock();
    if (count_ < numReqs) {
      char str_read[1048576];
      client->begin_tx();
      client->put(keys[count_], str, 1048576);
      auto res = client->getMeta(keys[count_]);
      // printf("put and get Meta of key %s\n", keys[count_].c_str());
      for (auto bm : res) {
        client->read(bm.address, str_read, bm.size);
        // printf("read of key %s, info is [%d]%ld-%d\n", keys[count_].c_str(),
        //       bm.r_key, bm.address, bm.size);
        auto res = memcmp(str, str_read, 1048576);
        if (res != 0) {
          fprintf(stderr,
                  "strcmp is %d, read res is not aligned with wrote. readed "
                  "content is \n",
                  res);
          for (int i = 0; i < 1048576; i++) {
            fprintf(stderr, "%X ", *(str_read + i));
          }
          fprintf(stderr, "\n wrote content is \n");
          for (int i = 0; i < 1048576; i++) {
            fprintf(stderr, "%X ", *(str + i));
          }
          fprintf(stderr, "\n");
        }
      }
      client->end_tx();
    } else {
      break;
    }
  }
}

int main(int argc, char** argv) {
  /// initialize Config class
  std::shared_ptr<Config> config = std::make_shared<Config>();
  CHK_ERR("config init", config->init(argc, argv));
  auto client =
      std::make_shared<PmPoolClient>(config->get_proxy_addrs(), config->get_proxy_port());
  char temp[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
                 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f'};
  for (int i = 0; i < 1048576 / 32; i++) {
    memcpy(str + i * 32, temp, 32);
  }
  for (int i = 0; i < 20480; i++) {
    keys.emplace_back("block_" + std::to_string(i));
  }
  client->init();

  int threads = 4;
  std::cout << "start put." << std::endl;
  std::vector<std::shared_ptr<std::thread>> threads_1;
  uint64_t start = timestamp_now();
  for (int i = 0; i < threads; i++) {
    auto t = std::make_shared<std::thread>(func1, client);
    threads_1.push_back(t);
  }
  for (auto thread : threads_1) {
    thread->join();
  }
  uint64_t end = timestamp_now();
  std::cout << "pmemkv put test: 1048576 "
            << " bytes test, consumes " << (end - start) / 1000.0
            << "s, throughput is " << numReqs / ((end - start) / 1000.0)
            << "MB/s" << std::endl;
  client.reset();
  /*for (int i = 0; i < 20480; i++) {
    client->begin_tx();
    client->del(keys[i]);
    client->end_tx();
  }
  std::cout << "freed." << std::endl;
  client->wait();*/
  return 0;
}
