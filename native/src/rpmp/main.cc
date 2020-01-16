/*
 * Filename: /mnt/spark-pmof/tool/rpmp/main.cc
 * Path: /mnt/spark-pmof/tool/rpmp
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include <memory>

#include "pmpool/Config.h"
#include "pmpool/DataServer.h"

int ServerMain(int argc, char **argv) {
  std::shared_ptr<Config> config = std::make_shared<Config>(argc, argv);
  // start data server
  std::shared_ptr<DataServer> dataServer =
      std::make_shared<DataServer>(config.get());
  dataServer->init();
  dataServer->wait();
  return 0;
}

int main(int argc, char **argv) {
  ServerMain(argc, argv);
  return 0;
}
