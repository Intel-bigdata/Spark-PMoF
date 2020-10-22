/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Config.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_CONFIG_H_
#define PMPOOL_CONFIG_H_

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

using namespace boost::program_options;
using namespace std;
/*using boost::program_options::error;
using boost::program_options::options_description;
using boost::program_options::value;
using boost::program_options::variables_map;
using std::string;
using std::vector;*/

/**
 * @brief This class represents the current RPMP configuration.
 *
 */
class Config {
 public:
  int init(int argc, char **argv) {
    try {
      options_description desc{"Options"};
      desc.add_options()("help,h", "Help screen")(
          "address,a", value<string>()->default_value("172.168.0.40"),
          "set the rdma server address")(
          "port,p", value<string>()->default_value("12346"),
          "set the rdma server port")("network_buffer_size,nbs",
                                      value<int>()->default_value(65536),
                                      "set network buffer size")(
          "network_buffer_num,nbn", value<int>()->default_value(16),
          "set network buffer number")("network_worker,nw",
                                       value<int>()->default_value(1),
                                       "set network wroker number")(
          "paths,ps", value<vector<string>>()->multitoken(),
          "set memory pool path")("sizes,ss",
                                  value<vector<uint64_t>>()->multitoken(),
                                  "set memory pool size")(
          "task_set, t", value<vector<int>>()->multitoken(),
          "set affinity for each device")(
          "log,l", value<string>()->default_value("/tmp/rpmp.log"),
          "set rpmp log file path")("log_level,ll",
                                    value<string>()->default_value("warn"),
                                    "set log level");

      command_line_parser parser{argc, argv};
      parsed_options parsed_options = parser.options(desc).run();
      variables_map vm;
      store(parsed_options, vm);
      notify(vm);

      if (vm.count("help")) {
        std::cout << desc << '\n';
        throw;
      }
      set_ip(vm["address"].as<string>());
      set_port(vm["port"].as<string>());
      set_network_buffer_size(vm["network_buffer_size"].as<int>());
      set_network_buffer_num(vm["network_buffer_num"].as<int>());
      set_network_worker_num(vm["network_worker"].as<int>());
      // pool_paths_.push_back("/dev/dax0.0");
      if (vm.count("sizes")) {
        set_pool_sizes(vm["sizes"].as<vector<uint64_t>>());
      }
      if (vm.count("paths")) {
        set_pool_paths(vm["paths"].as<vector<string>>());
      } else {
        std::cerr << "No PMem devices input, check '--paths' pls" << std::endl;
        std::cout << desc << '\n';
        throw;
      }
      if (pool_paths_.size() != sizes_.size()) {
        if (sizes_.size() < pool_paths_.size() && !sizes_.empty()) {
          auto first = sizes_[0];
          sizes_.resize(pool_paths_.size(), first);
        } else if (sizes_.size() > pool_paths_.size()) {
          sizes_.resize(pool_paths_.size());
        } else {
          std::cerr << "No size of PMem devices, check '--sizes' pls" << std::endl; 
          std::cout << desc << '\n';
          throw;
        }
      }
      if (vm.count("task_set")) {
        set_affinities_(vm["task_set"].as<vector<int>>());
      } else {
        affinities_.resize(pool_paths_.size(), -1);
      }
      set_log_path(vm["log"].as<string>());
      set_log_level(vm["log_level"].as<string>());
    } catch (const error &ex) {
      std::cerr << ex.what() << '\n';
    }
    return 0;
  }

  string get_ip() { return ip_; }
  void set_ip(string ip) { ip_ = ip; }

  string get_port() { return port_; }
  void set_port(string port) { port_ = port; }

  int get_network_buffer_size() { return network_buffer_size_; }
  void set_network_buffer_size(int network_buffer_size) {
    network_buffer_size_ = network_buffer_size;
  }

  int get_network_buffer_num() { return network_buffer_num_; }
  void set_network_buffer_num(int network_buffer_num) {
    network_buffer_num_ = network_buffer_num;
  }

  int get_network_worker_num() { return network_worker_num_; }
  void set_network_worker_num(int network_worker_num) {
    network_worker_num_ = network_worker_num;
  }

  vector<string> &get_pool_paths() { return pool_paths_; }
  void set_pool_paths(const vector<string> &pool_paths) {
    pool_paths_ = pool_paths;
  }

  std::vector<uint64_t> get_pool_sizes() { return sizes_; }
  void set_pool_sizes(vector<uint64_t> sizes) { sizes_ = sizes; }

  int get_pool_size() { return sizes_.size(); }

  void set_affinities_(vector<int> affinities) {
    if (affinities.size() < pool_paths_.size()) {
      affinities_.resize(pool_paths_.size(), -1);
    } else {
      for (int i = 0; i < pool_paths_.size(); i++) {
        affinities_.push_back(affinities[i]);
        std::cout << pool_paths_[i] << " task_set to " << affinities[i]
                  << std::endl;
      }
    }
  }
  std::vector<int> get_affinities_() { return affinities_; }

  string get_log_path() { return log_path_; }
  void set_log_path(string log_path) { log_path_ = log_path; }

  string get_log_level() { return log_level_; }
  void set_log_level(string log_level) { log_level_ = log_level; }

 private:
  string ip_;
  string port_;
  int network_buffer_size_;
  int network_buffer_num_;
  int network_worker_num_;
  vector<string> pool_paths_;
  vector<uint64_t> sizes_;
  vector<int> affinities_;
  string log_path_;
  string log_level_;
};

#endif  // PMPOOL_CONFIG_H_
