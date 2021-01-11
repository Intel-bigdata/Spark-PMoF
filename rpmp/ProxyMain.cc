#include <memory>

#include "pmpool/Config.h"
#include "pmpool/Proxy.h"
#include "pmpool/Log.h"

int main(int argc, char* argv[]){
  std::shared_ptr<Config> config = std::make_shared<Config>();
  if (argc > 1) {
    CHK_ERR("init config", config->init(argc, argv));
  } else {
    config->readFromFile();
  }
  std::shared_ptr<Log> log = std::make_shared<Log>(config.get());
  // ProxyServer* proxyServer;
  std::shared_ptr<Proxy> proxyServer = std::make_shared<Proxy>(config, log);
  proxyServer->launchServer();
  proxyServer->wait();
  return 0;
}