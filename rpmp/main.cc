/*
 * Copyright (c) 2019 Intel
 */

#include <memory>

#include "pmpool/Config.h"
#include "pmpool/DataServer.h"
#include "pmpool/Base.h"
#include "pmpool/RLog.h"

/**
 * @brief program entry of RPMP data server
 */
int ServerMain(int argc, char **argv) {
  /// initialize Config class
  std::shared_ptr<Config> config = std::make_shared<Config>();
  config->readFromFile();
  if (argc > 1){
    CHK_ERR("config init", config->init(argc, argv));
  }
  /// initialize Log class
  std::shared_ptr<RLog> log = std::make_shared<RLog>(config->get_log_path(), config->get_log_level());

  /// initialize DataServer class
  std::shared_ptr<DataServer> dataServer =
      std::make_shared<DataServer>(config, log);
  log->get_file_log()->info("start to initialize data server.");
  CHK_ERR("data server init", dataServer->init());
  log->get_file_log()->info("data server initialized.");
  dataServer->wait();
   
  return 0;
}

int main(int argc, char **argv) {
  ServerMain(argc, argv);
  return 0;
}
