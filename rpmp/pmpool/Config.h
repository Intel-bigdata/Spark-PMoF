/*
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_CONFIG_H_
#define PMPOOL_CONFIG_H_

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <map>

#include <boost/program_options.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

using namespace boost::program_options;
using namespace std;

#include <fstream>
#include <sstream>
#include <string>

/**
 * @brief This class represents the current RPMP configuration.
 *
 */
class Config {
private:
    map<std::string,std::string> configs;
public:
    int setDefault(){
      configs.insert(pair<string,string>(RPMP_NODE_LIST, DEFAULT_RPMP_NODE_LIST));  
      configs.insert(pair<string,string>(RPMP_NETWORK_HEARTBEAT_INTERVAL, DEFAULT_RPMP_NETWORK_HEARTBEAT_INTERVAL));  
      configs.insert(pair<string,string>(RPMP_NETWORK_PROXY_ADDRESS, DEFAULT_RPMP_NETWORK_PROXY_ADDRESS));  
      configs.insert(pair<string,string>(RPMP_PROXY_CLIENT_SERVICE_PORT, DEFAULT_RPMP_PROXY_CLIENT_SERVICE_PORT));
      configs.insert(pair<string,string>(RPMP_NETWORK_SERVER_ADDRESS, DEFAULT_RPMP_NETWORK_SERVER_ADDRESS));
      configs.insert(pair<string,string>(RPMP_NETWORK_SERVER_PORT, DEFAULT_RPMP_NETWORK_SERVER_PORT));  
      configs.insert(pair<string,string>(RPMP_NETWORK_WORKER, DEFAULT_RPMP_NETWORK_WORKER));  
      configs.insert(pair<string,string>(RPMP_STORAGE_NAMESPACE_SIZE, DEFAULT_RPMP_STORAGE_NAMESPACE_SIZE));  
      configs.insert(pair<string,string>(RPMP_STORAGE_NAMESPACE_LIST, DEFAULT_RPMP_STORAGE_NAMESPACE_LIST));  
      configs.insert(pair<string,string>(RPMP_TASK_LIST, DEFAULT_RPMP_TASK_LIST));  
      configs.insert(pair<string,string>(RPMP_NETWORK_BUFFER_NUMBER, DEFAULT_RPMP_NETWORK_BUFFER_NUMBER));
      configs.insert(pair<string,string>(RPMP_NETWORK_BUFFER_SIZE, DEFAULT_RPMP_NETWORK_BUFFER_SIZE));
      configs.insert(pair<string,string>(RPMP_LOG_LEVEL, DEFAULT_RPMP_LOG_LEVEL));
      configs.insert(pair<string,string>(RPMP_LOG_PATH, DEFAULT_RPMP_LOG_PATH));
      configs.insert(pair<string,string>(RPMP_DATA_REPLICA, DEFAULT_RPMP_DATA_REPLICA));
      configs.insert(pair<string,string>(RPMP_DATA_MINREPLICA, DEFAULT_RPMP_DATA_MINREPLICA));
      configs.insert(pair<string,string>(RPMP_PROXY_REPLICA_SERVICE_PORT, DEFAULT_RPMP_PROXY_REPLICA_SERVICE_PORT));
      configs.insert(pair<string,string>(RPMP_PROXY_LOAD_BALANCE_FACTOR, DEFAULT_RPMP_PROXY_LOAD_BALANCE_FACTOR));
      configs.insert(pair<string,string>(RPMP_METASTORE_REDIS_IP, DEFAULT_RPMP_METASTORE_REDIS_IP));
      configs.insert(pair<string,string>(RPMP_METASTORE_REDIS_PORT, DEFAULT_RPMP_METASTORE_REDIS_PORT));
      return 0;
    }

    /// This path depends on where user launches proxy or data server. For manually launching, user should go to
    /// $RPMP_HOME/bin to start proxy or data server.
    int readFromFile(string file = "../config/rpmp.conf"){
      setDefault();
      std::ifstream infile(file);
      if (infile.is_open()){
        std::string line;
        while (std::getline(infile, line)){
          std::string buf;
          std::vector<std::string> tokens;    
          std::stringstream ss(line);
          bool key = true;
          std::string previousKey;
          while (ss >> buf){
            if (key){
              configs[buf] = "";
              previousKey = buf;
              key = false;
            }else{
              configs[previousKey] = buf; 
            }
          }
        }

        set_nodes(configs.find(RPMP_NODE_LIST)->second);

        set_replica_service_port(configs.find(RPMP_PROXY_REPLICA_SERVICE_PORT)->second);

        set_load_balance_factor(stoi(configs.find(RPMP_PROXY_LOAD_BALANCE_FACTOR)->second));

        set_data_minReplica(stoi(configs.find(RPMP_DATA_MINREPLICA)->second));

        set_data_replica(stoi(configs.find(RPMP_DATA_REPLICA)->second));

        set_heartbeat_interval(stoi(configs.find(RPMP_NETWORK_HEARTBEAT_INTERVAL)->second));

        set_heartbeat_port(configs.find(RPMP_NETWORK_HEARTBEAT_PORT)->second);

        set_proxy_addrs(configs.find(RPMP_NETWORK_PROXY_ADDRESS)->second);

        set_ip(configs.find(RPMP_NETWORK_SERVER_ADDRESS)->second);

        set_client_service_port(configs.find(RPMP_PROXY_CLIENT_SERVICE_PORT)->second);

        set_port(configs.find(RPMP_NETWORK_SERVER_PORT)->second);

        set_network_buffer_size(stoi(configs.find(RPMP_NETWORK_BUFFER_SIZE)->second)); 

        set_network_buffer_num(stoi(configs.find(RPMP_NETWORK_BUFFER_NUMBER)->second));

        set_network_worker_num(stoi(configs.find(RPMP_NETWORK_WORKER)->second));

        set_metastore_redis_ip(configs.find(RPMP_METASTORE_REDIS_IP)->second);
        set_metastore_redis_port(configs.find(RPMP_METASTORE_REDIS_PORT)->second); 

        set_metastore_type(configs.find(RPMP_METASTORE_TYPE)->second);
        set_node_connect_timeout(stoi(configs.find(RPMP_PROXY_NODE_CONNECT_TIMEOUT)->second));

        set_log_path(configs.find(RPMP_LOG_PATH)->second);
        set_log_level(configs.find(RPMP_LOG_LEVEL)->second);
        string sizes = configs.find(RPMP_STORAGE_NAMESPACE_SIZE)->second;
        string delimiter = ",";
        int start = 0; 
        int end = sizes.find(delimiter);
        while (end != string::npos){
          sizes_.push_back(stoull(sizes.substr(start, end-start)));
          start = end + delimiter.length();
          end = sizes.find(delimiter, start);
        }        
        sizes_.push_back(stoull(sizes.substr(start,end)));

        string paths = configs.find(RPMP_STORAGE_NAMESPACE_LIST)->second;
        int start_path = 0;
        int end_path = paths.find(delimiter); 
        while (end_path != string::npos){
          pool_paths_.push_back(paths.substr(start_path, end_path - start_path));
          start_path = end_path + delimiter.length();
          end_path = paths.find(delimiter, start_path);
        }
        pool_paths_.push_back(paths.substr(start_path, end_path));

        if (pool_paths_.size() != sizes_.size()) {
          if (sizes_.size() < pool_paths_.size() && !sizes_.empty()) {
            auto first = sizes_[0];
            sizes_.resize(pool_paths_.size(), first);
          } else if (sizes_.size() > pool_paths_.size()) {
            sizes_.resize(pool_paths_.size());
          } else {
            std::cerr << "No size of PMem devices specified." << std::endl;
            throw;
          }
        }

        string tasks = configs.find(RPMP_TASK_LIST)->second;
        int start_task = 0;
        int end_task = tasks.find(delimiter);

        while(end_task != string::npos){
          affinities_.push_back(stoi(tasks.substr(start_task, end_task - start_task))); 
          start_task = end_task + delimiter.length();
          end_task = tasks.find(delimiter, start_task);
        }
        affinities_.push_back(stoi(tasks.substr(start_task, end_task)));
      }
      return 0;
    }

    /**
     * Generally it is recommended that init function is called after readFromFile function
     * to get not configured properties initialized with default value.
     */
    int init(int argc, char **argv) {
      try {
        options_description desc{"Options"};
        desc.add_options()("help,h", "Help screen")(
            "address,a", value<string>()->default_value("0.0.0.0"),
            "set the rdma server address")(
              "port,p", value<string>()->default_value("12346"),
              "set the rdma server port")("network_buffer_size,nbs",
                value<int>()->default_value(65536),
                "set network buffer size")(
                  "network_buffer_num,nbn", value<int>()->default_value(16),
                  "set network buffer number")("network_worker,nw",
                    value<int>()->default_value(1),
                    "set network worker number")(
                      "paths,ps", value<vector<string>>()->multitoken(),
                      "set memory pool path")("sizes,ss",
                        value<vector<uint64_t>>()->multitoken(),
                        "set memory pool size")(
                          "task_set, t", value<vector<int>>()->multitoken(),
                          "set affinity for each device")(
                            "log,l", value<string>()->default_value("/tmp/rpmp.log"),
                            "set rpmp log file path")("log_level,ll",
                              value<string>()->default_value("warn"),
                              "set log level")("current_proxy_addr,cpa",
                                  value<string>()->default_value("0.0.0.0"),
                                  "Set current proxy address, applicable to proxy node."
                                  );

        command_line_parser parser{argc, argv};
        parsed_options parsed_options = parser.options(desc).run();
        variables_map vm;
        store(parsed_options, vm);
        notify(vm);

        if (vm.count("help")) {
          std::cout << desc << '\n';
          throw;
        }
        /// * If property is not set, set with command line value or default value.
        /// * If property is set and command line value is not default value, set it.
        /// * If property is set, but command line value is default value, do not set it.
        /// * If property is not set in config file, no need to check above conditions, e.g., #set_affinities,
        ///   #set_current_proxy_addr.
        /// The consideration is if property is set with a not default value from config file in #readFromFile,
        /// it should not be reset with a default value in #init. And #init can override property value set by
        /// #readFromFile if it has not default value.
        set_ip(vm["address"].as<string>());
        set_port(vm["port"].as<string>());
        set_network_buffer_size(vm["network_buffer_size"].as<int>());
        set_network_buffer_num(vm["network_buffer_num"].as<int>());
        set_network_worker_num(vm["network_worker"].as<int>());
        // Applicable to proxy node.
        if (vm.count("current_proxy_addr")) {
          set_current_proxy_addr(vm["current_proxy_addr"].as<string>());
        }
        // pool_paths_.push_back("/dev/dax0.0");
        if (vm.count("sizes")) {
          set_pool_sizes(vm["sizes"].as<vector<uint64_t>>());
        }
        if (vm.count("paths")) {
          set_pool_paths(vm["paths"].as<vector<string>>());
        } else if (pool_paths_.empty()) {
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
          set_affinities(vm["task_set"].as<vector<int>>());
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
    void set_ip(string ip) {
      if (ip_ == "") {
        ip_ = ip;
        return;
      }
      if (ip != DEFAULT_RPMP_NETWORK_SERVER_ADDRESS) {
        ip_ = ip;
      }
    }

    string get_port() { return port_; }
    void set_port(string port) {
      if (port_ == "") {
        port_ = port;
        return;
      }
      if (port != DEFAULT_RPMP_NETWORK_SERVER_PORT) {
        port_ = port;
      }
    }

    int get_network_buffer_size() { return network_buffer_size_; }
    void set_network_buffer_size(int network_buffer_size) {
      if (network_buffer_size_ == 0) {
        network_buffer_size_ = network_buffer_size;
        return;
      }
      if (network_buffer_size != stoi(DEFAULT_RPMP_NETWORK_BUFFER_SIZE)) {
        network_buffer_size_ = network_buffer_size;
      }
    }

    int get_network_buffer_num() { return network_buffer_num_; }
    void set_network_buffer_num(int network_buffer_num) {
      if (network_buffer_num_ == 0) {
        network_buffer_num_ = network_buffer_num;
        return;
      }
      if (network_buffer_num != stoi(DEFAULT_RPMP_NETWORK_BUFFER_NUMBER)) {
        network_buffer_num_ = network_buffer_num;
      }
    }

    int get_network_worker_num() { return network_worker_num_; }
    void set_network_worker_num(int network_worker_num) {
      if (network_worker_num_ == 0) {
        network_worker_num_ = network_worker_num;
        return;
      }
      if (network_worker_num != stoi(DEFAULT_RPMP_NETWORK_WORKER)) {
        network_worker_num_ = network_worker_num;
      }
    }

    vector<string> &get_pool_paths() { return pool_paths_; }
    void set_pool_paths(const vector<string> &pool_paths) {
      if (pool_paths_.empty()) {
        pool_paths_ = pool_paths;
        return;
      }
      if (!pool_paths.empty()) {
        pool_paths_ = pool_paths;
      }
    }

    std::vector<uint64_t> get_pool_sizes() { return sizes_; }
    void set_pool_sizes(vector<uint64_t> sizes) {
      if (sizes_.empty()) {
        sizes_ = sizes;
        return;
      }
      if (!sizes.empty()) {
        sizes_ = sizes;
      }
    }

    int get_pool_size() { return sizes_.size(); }

    void set_affinities(vector<int> affinities) {
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
    void set_log_path(string log_path) {
      if (log_path_ == "") {
        log_path_ = log_path;
        return;
      }
      if (log_path != DEFAULT_RPMP_LOG_PATH) {
        log_path_ = log_path;
      }
    }

    string get_log_level() { return log_level_; }
    void set_log_level(string log_level) {
      if (log_level_ == "") {
        log_level_ = log_level;
        return;
      }
      if (log_level != DEFAULT_RPMP_LOG_LEVEL) {
        log_level_ = log_level;
      }
    }

    void set_nodes(string configured_nodes) {
      vector<string> nodes;
      boost::split(nodes, configured_nodes, boost::is_any_of(","), boost::token_compress_on);
      nodes_ = nodes;
    }

    vector <string> get_nodes() { return nodes_; }

    void set_client_service_port(string port) { proxy_client_service_port_ = port; }

    string get_client_service_port() { return proxy_client_service_port_; }

    void set_proxy_addrs(string proxy_addrs) {
      vector<string> proxies;
      boost::split(proxies, proxy_addrs, boost::is_any_of(","), boost::token_compress_on);
      proxy_addrs_ = proxies;
    }
    vector<string> get_proxy_addrs() {
      return proxy_addrs_;
    }

    void set_current_proxy_addr(string current_proxy_addr) {
      current_proxy_addr_ = current_proxy_addr;
    }
    string get_current_proxy_addr() {
      return current_proxy_addr_;
    }

    void set_data_replica(uint32_t replica) {replica_ = replica;}
    uint32_t get_data_replica() {return replica_;}

    void set_data_minReplica(uint32_t replica) {minReplica_ = replica;}
    uint32_t get_data_minReplica() {return minReplica_;}

    void set_replica_service_port(string port) {proxy_replica_service_port_ = port;}
    string get_replica_service_port() {return proxy_replica_service_port_;}

    void set_load_balance_factor(uint32_t factor) {load_balance_factor_ = factor;}
    uint32_t get_load_balance_factor() {return load_balance_factor_;}

    void set_heartbeat_interval(int heartbeatInterval) {heartbeat_interval_ = heartbeatInterval;}
    int get_heartbeat_interval() {return heartbeat_interval_;}

    void set_heartbeat_port(string heartbeat_port){heartbeat_port_ = heartbeat_port;}
    string get_heartbeat_port(){return heartbeat_port_;}

    void set_metastore_redis_ip(string redis_ip){redis_ip_ = redis_ip;};
    string get_metastore_redis_ip(){return redis_ip_; };

    void set_metastore_redis_port(string redis_port){redis_port_ = redis_port;};
    string get_metastore_redis_port(){return redis_port_;};

    void set_metastore_type(string metastore_type){metastore_type_ = metastore_type;};
    string get_metastore_type(){return metastore_type_;};
    
    void set_node_connect_timeout(int proxy_node_connect_timeout){proxy_node_connect_timeout_ = proxy_node_connect_timeout;};
    int get_node_connect_timeout(){return proxy_node_connect_timeout_;};

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
    vector<string> nodes_;
    int heatbeat_interval_;
    string proxy_client_service_port_;
    // Applicable to proxy node.
    string current_proxy_addr_;
    vector<string> proxy_addrs_;
    uint32_t replica_;
    uint32_t minReplica_;
    string proxy_replica_service_port_;
    uint32_t load_balance_factor_;
    int heartbeat_interval_;
    string heartbeat_port_;
    string redis_ip_;
    string redis_port_;
    string metastore_type_;
    int proxy_node_connect_timeout_;

    const string RPMP_NODE_LIST = "rpmp.node.list";
    const string RPMP_NETWORK_HEARTBEAT_INTERVAL = "rpmp.network.heartbeat-interval.sec";
    const string RPMP_NETWORK_HEARTBEAT_PORT = "rpmp.network.heartbeat.port";
    const string RPMP_NETWORK_PROXY_ADDRESS = "rpmp.network.proxy.address";
    const string RPMP_PROXY_CLIENT_SERVICE_PORT = "rpmp.proxy.client.service.port";
    const string RPMP_NETWORK_SERVER_ADDRESS = "rpmp.network.server.address";
    const string RPMP_NETWORK_SERVER_PORT = "rpmp.network.server.port";
    const string RPMP_NETWORK_WORKER = "rpmp.network.worker";
    const string RPMP_STORAGE_NAMESPACE_SIZE = "rpmp.storage.namespace.size";
    const string RPMP_STORAGE_NAMESPACE_LIST = "rpmp.storage.namespace.list";
    const string RPMP_TASK_LIST = "rpmp.task.set";
    const string RPMP_NETWORK_BUFFER_NUMBER = "rpmp.network.buffer.number";
    const string RPMP_NETWORK_BUFFER_SIZE = "rpmp.network.buffer.size";
    const string RPMP_LOG_LEVEL = "rpmp.log.level";
    const string RPMP_LOG_PATH = "rpmp.log.path";
    const string RPMP_DATA_REPLICA = "rpmp.data.replica";
    const string RPMP_DATA_MINREPLICA = "rpmp.data.min.replica";
    const string RPMP_PROXY_REPLICA_SERVICE_PORT = "rpmp.proxy.replica.service.port";
    const string RPMP_PROXY_LOAD_BALANCE_FACTOR = "rpmp.proxy.load-balance-factor";
    const string RPMP_METASTORE_REDIS_IP = "rpmp.metastore.redis.ip";
    const string RPMP_METASTORE_REDIS_PORT = "rpmp.metastore.redis.port";
    const string RPMP_METASTORE_TYPE = "rpmp.metastore.type";
    const string RPMP_PROXY_NODE_CONNECT_TIMEOUT = "rpmp.proxy.node.connect.timeout";

    const string DEFAULT_RPMP_NODE_LIST = "0.0.0.0";
    const string DEFAULT_RPMP_NETWORK_HEARTBEAT_INTERVAL = "3";
    const string DEFAULT_RPMP_NETWORK_HEARTBEAT_PORT = "12355";
    const string DEFAULT_RPMP_NETWORK_PROXY_ADDRESS = "0.0.0.0";
    const string DEFAULT_RPMP_PROXY_CLIENT_SERVICE_PORT = "12350";
    const string DEFAULT_RPMP_NETWORK_SERVER_ADDRESS = "0.0.0.0";
    const string DEFAULT_RPMP_NETWORK_SERVER_PORT = "12346";
    const string DEFAULT_RPMP_NETWORK_WORKER = "10";
    const string DEFAULT_RPMP_STORAGE_NAMESPACE_SIZE = "rpmp.storage.namespace.list";
    const string DEFAULT_RPMP_STORAGE_NAMESPACE_LIST = "/dev/dax0.0,/dev/dax0.1,/dev/dax1.0,/dev/dax1.1";
    const string DEFAULT_RPMP_TASK_LIST = "2,38,20,56";
    const string DEFAULT_RPMP_NETWORK_BUFFER_NUMBER = "16";
    const string DEFAULT_RPMP_NETWORK_BUFFER_SIZE = "65536";
    const string DEFAULT_RPMP_LOG_LEVEL = "warn";
    const string DEFAULT_RPMP_LOG_PATH = "/tmp/rpmp.log";
    const string DEFAULT_RPMP_DATA_REPLICA = "3";
    const string DEFAULT_RPMP_DATA_MINREPLICA = "1";
    const string DEFAULT_RPMP_PROXY_REPLICA_SERVICE_PORT = "12340";
    const string DEFAULT_RPMP_PROXY_LOAD_BALANCE_FACTOR = "5";
    const string DEFAULT_RPMP_METASTORE_REDIS_IP = "0.0.0.0";
    const string DEFAULT_RPMP_METASTORE_REDIS_PORT = "6379";
    const string DEFAULT_RPMP_METASTORE_TYPE = "REDIS";
    const string DEFAULT_RPMP_PROXY_NODE_CONNECT_TIMEOUT = "30";
};

#endif  // PMPOOL_CONFIG_H_
