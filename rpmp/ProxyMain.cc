#include <memory>

#include "pmpool/Config.h"
#include "pmpool/Proxy.h"
#include "pmpool/RLog.h"

int main(int argc, char* argv[]){
  std::shared_ptr<Config> config = std::make_shared<Config>();
  config->readFromFile();
  if (argc > 1) {
    CHK_ERR("init config", config->init(argc, argv));
  }
  std::shared_ptr<RLog> log = std::make_shared<RLog>(config->get_log_path(), config->get_log_level());
  // Host ip will be passed from command line through a launch script.
  // It is consistent with user-specified address in the configuration.
  std::string currentHostAddr = config->get_current_proxy_addr();
  std::shared_ptr<Proxy> proxyServer = std::make_shared<Proxy>(config, log, currentHostAddr);
  if (!proxyServer->launchServer()) {
    log->get_console_log()->error("Failed to launch proxy server!");
    return -1;
  }
  proxyServer->wait();
  return 0;
}