#include "libpmemobj_write_bench.h"
#include <iostream>
#include <string>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <mutex>
#include <condition_variable>
#include <thread>

#include <libpmemobj.h>
#include <errno.h>
#include <fcntl.h>

using namespace std;

ArgParser::ArgParser(int ac, char* av[]) {
    boost::program_options::options_description options("options");
    options.add_options()
        ("help,h", "Print help messages")
        ("device,d", boost::program_options::value<string>(), "pmem device path")
        ("runtime,r", boost::program_options::value<string>(), "runtime")
        ("block_size,b", boost::program_options::value<string>(), "block size(KB)");
	    boost::program_options::store(boost::program_options::parse_command_line(ac, av, options), vm);
    if (vm.count("help")){ 
        std::cout << "Basic Command Line Parameter App" << std::endl 
              << options << std::endl; 
        exit(1);
    } 
	    boost::program_options::notify(vm);
}

string ArgParser::get(string key) {
    if (vm.count(key)) {
        return vm[key].as<string>();
    }
    return ""; 
}

Monitor::Monitor(int runtime, int bs):
    committed_jobs(0),
    block_size(bs),
    alive(true) {
    run_thread = new thread(&Monitor::run, this);
}

Monitor::~Monitor() {
    delete run_thread;
}

void Monitor::incCommittedJobs() {
    std::lock_guard<std::mutex> lock(committed_jobs_mutex);
    committed_jobs += 1;
}

bool Monitor::is_alive() {
    return alive;
}

void Monitor::stop() {
    alive = false;
    run_thread->join();
    elapse_sec -= 1;
    printf("======= Summarize =======\nAvg Bandwidth: %d (MB/s)\nAvg IOPS: %d\nTotal write data:%dMB\n", (committed_jobs * block_size / 1024 / elapse_sec), (committed_jobs / elapse_sec), (committed_jobs * block_size) / 1024);
}

void Monitor::run() {
    long last_committed_jobs = 0;
    while (alive) {
        printf("Second %d (MB/s): %d\n", elapse_sec, (committed_jobs - last_committed_jobs) * block_size / 1024);
	        last_committed_jobs = committed_jobs;
	        elapse_sec += 1;
	        sleep(1);
	    }
}

Writer::Writer(Monitor* mon, const char* dev, int bs, int thread_num, unsigned core):
    mon(mon),
    block_size(bs),
    pmpool(dev, 100, 100, 10, 20){
    thread_pool.push_back(thread(&Writer::write, this));
}

void Writer::stop() {
    alive = false;
}

Writer::~Writer() {
}

void Writer::write() {
  char *data = new char[block_size * 1024];
  int i = 0, j = 0, k = 0;
  while (mon->is_alive()) {
      if (i > 99) {
          i = 0;
          j += 1;
      }
      if (j > 99) {
          j = 0;
          k += 1;
      }
      if (k > 99) {
          break;
      }
      pmpool.setPartition(100, i, j, k, block_size * 1024, data);
      i++;
      mon->incCommittedJobs();
  }
  delete data;
}

std::vector<std::string> split_string_to_vector(const string& s, char delimiter ) {
   std::vector<std::string> tokens;
   std::string token;
   std::istringstream tokenStream(s);
   while (std::getline(tokenStream, token, delimiter))
   {
      tokens.push_back(token);
   }
   return tokens;
}

int main(int argc, char* argv[]) {
    ArgParser arg_parser(argc, argv);
    int thread_num = 1;
    int taskset = 10;
    int runtime = stoi(arg_parser.get("runtime"));
    int bs = stoi(arg_parser.get("block_size"));
    vector<string> device_list = split_string_to_vector(arg_parser.get("device"), ',');

    Monitor monitor(runtime, bs);
    vector<Writer*> writer_list;
    for (int i = 0; i < device_list.size(); i++) {
        writer_list.push_back(new Writer(&monitor, device_list[i].c_str(), bs, thread_num, (unsigned)taskset));
        taskset += thread_num;
    }
    sleep(runtime);
    monitor.stop();
    for (int i = 0; i < device_list.size(); i++) {
        writer_list[i]->stop();
        delete writer_list[i];
    }
    exit(0);
}

