#include <string>
#include <cstdlib>
#include <thread>  // NOLINT
#include "Config.h"
#include "pmpool/Base.h"
#include "pmpool/client/PmPoolClient.h"

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
    std::chrono::milliseconds(1);
}

const int char_size = 1048576;

char str[char_size];

void put(int map_id, int start, int end, std::shared_ptr<PmPoolClient> client) {
  int count = start;
  while (count < end) {
    std::string key =
      "block_" + std::to_string(map_id) + "_" + std::to_string(count++);
    client->begin_tx();
    client->put(key, str, char_size);
    client->end_tx();
  }
}

int main(int argc, char** argv) {
  /// initialize Config class
  std::shared_ptr<Config> config = std::make_shared<Config>();
  CHK_ERR("config init", config->init(argc, argv));

  char temp[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
    'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
    'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f'};
  for (int i = 0; i < char_size/ 32; i++) {
    memcpy(str + i * 32, temp, 32);
  }

  int threads = config->get_num_threads();
  int map_id = config->get_map_id();
  int numReqs = config->get_num_reqs();
  std::string proxy_addrs = config->get_proxy_addrs();
  std::string proxy_port = config->get_proxy_port();

  std::cout << "=================== Put ======================="
    << std::endl;
  std::cout << "RPMP proxy address(es): " << proxy_addrs << std::endl;
  std::cout << "RPMP proxy port: " << proxy_port << std::endl;
  std::cout << "Total Num Requests is " << numReqs << std::endl;
  std::cout << "Total Num Threads is " << threads << std::endl;
  std::cout << "Block key pattern is "
    << "block_" << map_id << "_*" << std::endl;

  auto client = std::make_shared<PmPoolClient>(proxy_addrs, proxy_port);
  client->init();
  std::cout << "start put." << std::endl;
  int start = 0;
  int step = numReqs / threads;
  std::vector<std::shared_ptr<std::thread>> threads_1;
  uint64_t begin = timestamp_now();
  for (int i = 0; i < threads; i++) {
    auto t =
      std::make_shared<std::thread>(put, map_id, start, start + step, client);
    threads_1.push_back(t);
    start += step;
  }
  for (auto thread : threads_1) {
    thread->join();
  }
  uint64_t end = timestamp_now();
  std::cout << "[block_" << map_id << "_*]"
    << "pmemkv put test:  " << std::to_string(char_size) <<" "
    << " bytes test, consumes " << (end - begin) / 1000.0
    << "s, throughput is " << numReqs / ((end - begin) / 1000.0)
    << "MB/s" << std::endl;

  client.reset();
  return 0;
}