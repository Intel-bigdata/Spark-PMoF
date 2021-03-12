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
  // Host ip will be passed from command line through a launch script.
  // It is consistent with user-specified ip in the configuration.
  std::string currentHostAddr;
  // Directly launch proxy case.
  if (argc < 2) {
      currentHostAddr = "";
  }
  currentHostAddr = argv[1];
  std::shared_ptr<Proxy> proxyServer = std::make_shared<Proxy>(config, log, redis, currentHostAddr);
  proxyServer->launchServer();
  proxyServer->wait();
  return 0;
}