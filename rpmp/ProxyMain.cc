#include <memory>

#include "pmpool/Config.h"
#include "pmpool/Proxy.h"
#include "pmpool/Log.h"

int main(int argc, char* argv[]){
  std::shared_ptr<Config> config = std::make_shared<Config>();
  config->readFromFile();
  if (argc > 1) {
    CHK_ERR("init config", config->init(argc, argv));
  }
  std::shared_ptr<Log> log = std::make_shared<Log>(config.get());
  std::shared_ptr<Redis> redis = std::make_shared<Redis>(config, log);
  std::shared_ptr<Proxy> proxyServer = std::make_shared<Proxy>(config, log, redis);
  // Host ip will be passed from command line through a launch script.
  // It is consistent with user-specified ip in the configuration.
  string current_host_ip;
  if (argc < 2) {
    current_host_ip = "";
  }
  current_host_ip = argv[1];
  proxyServer->launchServer(current_host_ip);
  proxyServer->wait();
  return 0;
}