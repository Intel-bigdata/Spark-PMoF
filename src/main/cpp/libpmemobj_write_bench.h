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

#include "PersistentMemoryPool.h"
#include <errno.h>
#include <fcntl.h>

using namespace std;

class ArgParser {
public:
    ArgParser(int ac, char* av[]);
    string get(string key);
private:
    boost::program_options::variables_map vm;
};

class Monitor {
public:
    Monitor(int runtime, int bs = 2);
    ~Monitor();
    void incCommittedJobs();
    bool is_alive();
    void stop();
private:
    void run();
    thread *run_thread;
    uint64_t committed_jobs;
    int block_size;
    std::mutex committed_jobs_mutex;
    bool alive;
	int elapse_sec = 0;
};

class Writer {
public:
    Writer(Monitor* mon, const char* dev, int bs, int thd_cnt, unsigned core);
    ~Writer();
    //void write(int block_size, char* data);
    void write();
    void stop();

private:
    PMPool pmpool;
    vector<thread> thread_pool;
    Monitor* mon;
    bool alive = true;
    int block_size;
    int inflight = 0;
    std::mutex mtx;
    std::condition_variable cv;
};
