#ifndef PMPOOL_CONFIG_H_
#define PMPOOL_CONFIG_H_

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

using boost::program_options::error;
using boost::program_options::options_description;
using boost::program_options::value;
using boost::program_options::variables_map;
using std::string;
using std::vector;

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
          "proxy_addr,a", value<string>()->default_value("0.0.0.0"),
          "Set RPMP proxy server address(es). Please combine all proxy addresses with comma separated "
          "if HA mode is enabled.")(
          "port,p", value<string>()->default_value("12350"),
          "Set RPMP client service port of proxy server, consistent with the value set "
          "for `rpmp.proxy.client.service.port` on proxy side")(
          "log,l", value<string>()->default_value("/tmp/rpmp.log"),
          "Set rpmp log file path")("map_id,m", value<int>()->default_value(0),
                                    "Set map id")(
          "req_num,r", value<int>()->default_value(2048), "Set number of requests")(
          "threads,t", value<int>()->default_value(8), "Set number of threads");

      variables_map vm;
      store(parse_command_line(argc, argv, desc), vm);
      notify(vm);

      if (vm.count("help")) {
        std::cout << desc << '\n';
        return -1;
      }
      if (vm["proxy_addr"].as<string>().empty()) {
        std::cout << "Please specify RPMP proxy address(es)!\n";
        return -1;
      }

      set_proxy_addrs(vm["proxy_addr"].as<string>());
      set_proxy_port(vm["port"].as<string>());
      set_log_path(vm["log"].as<string>());
      set_map_id(vm["map_id"].as<int>());
      set_num_reqs(vm["req_num"].as<int>());
      set_num_threads(vm["threads"].as<int>());
    } catch (const error &ex) {
      std::cerr << ex.what() << '\n';
    }
    return 0;
  }

  int get_map_id() { return map_id_; }
  void set_map_id(int map_id) { map_id_ = map_id; }

  int get_num_reqs() { return num_reqs_; }
  void set_num_reqs(int num_reqs) { num_reqs_ = num_reqs; }

  int get_num_threads() { return num_threads_; }
  void set_num_threads(int num_threads) { num_threads_ = num_threads; }

  string get_proxy_addrs() { return proxy_addrs_; }
  void set_proxy_addrs(string proxy_addrs) { proxy_addrs_ = proxy_addrs; }

  string get_proxy_port() { return proxy_port_; }
  void set_proxy_port(string port) { proxy_port_ = port; }

  string get_log_path() { return log_path_; }
  void set_log_path(string log_path) { log_path_ = log_path; }

  string get_log_level() { return log_level_; }
  void set_log_level(string log_level) { log_level_ = log_level; }

 private:
  string proxy_addrs_;
  string proxy_port_;
  string log_path_;
  string log_level_;
  int map_id_ = 0;
  int num_threads_ = 8;
  int num_reqs_ = 2048;
};

#endif  // PMPOOL_CONFIG_H_
