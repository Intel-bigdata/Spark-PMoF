#include <string>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <mutex>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <thread>
#include <errno.h>
#include <fcntl.h>
#include <cassert>
#include <libpmemobj.h>
#include "WorkQueue.h"

/* This class is used to make PM as a huge pool
 * All shuffle files will be stored in the same pool
 * Default Maximun shuffle file will set to 1000
 * Then follows shuffle block array
 *
 * Data Structure
 * =============================================
 * ===stage_array[0]===
 * 0        shuffle_array[0].address  # contains all maps in this stage
 * 1        shuffle_array[1].address
 * ... ...
 * stage_id: 1000     shuffle_array[999].address
 * ===shuffle_array[0]===
 * 0      shuffle_block_array[0]   # contains all partitions in the shuffle map
 * 1      shuffle_block_array[1]
 * ... ...
 * ===shuffle_block[0]===
 * 0      partition[0].address, partition[0].size
 * 1      partition[1].address, partition[1].size
 * ... ...
 * partition[0]: 2MB block -> 2MB Block -> ...
 * partition[1]
 * ... ...
 * =============================================
 * */

using namespace std;

#define TOID_ARRAY_TYPE(x) TOID(x)
#define TOID_ARRAY(x) TOID_ARRAY_TYPE(TOID(x))

POBJ_LAYOUT_BEGIN(PersistentMemoryStruct);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayRoot);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct MapArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct MapArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionBlock);
POBJ_LAYOUT_END(PersistentMemoryStruct);

struct StageArrayRoot {
    TOID(struct StageArray) stageArray;
};

struct StageArray {
    int size;
    TOID(struct StageArrayItem) items[];
};

struct StageArrayItem {
    TOID(struct MapArray) mapArray;
};

struct MapArray {
    int size;
    TOID(struct MapArrayItem) items[];
};

struct MapArrayItem {
    TOID(struct PartitionArray) partitionArray;
};

struct PartitionArray {
    int size;
    TOID(struct PartitionArrayItem) items[];
};

struct PartitionArrayItem {
    TOID(struct PartitionBlock) first_block;
    long partition_size;
};

struct PartitionBlock {
    TOID(struct PartitionBlock) next_block;
    long data_size;
    PMEMoid data;
};

struct MemoryBlock {
    char* buf;
    MemoryBlock() {
    }

    ~MemoryBlock() {
        delete buf;
    }
};

class PMPool {
public:
    PMEMobjpool *pmpool;
    TOID(struct StageArrayRoot) stageArrayRoot;
    int maxStage;
    int maxMap;
    std::mutex pmem_index_lock;
    int core_s;
    int core_e;
    std::thread worker;
    bool stop;
    std::string device;
    WorkQueue<void*> request_queue;
    PMPool(const char* dev, int maxStage, int maxMap, int core_s, int core_e);
    ~PMPool();
    long setPartition(int partitionNum, int stageId, int mapId, int partitionId, long size, char* data );
    long getPartition(MemoryBlock* mb, int stageId, int mapId, int partitionId);
    void process();
};

class Request {
public:
    // add lock to make this request blocked
    std::mutex mtx;
    std::condition_variable cv;
    std::unique_lock<std::mutex> lck;

    // add lock to make func blocked
    std::mutex block_mtx;
    std::condition_variable block_cv;
    std::unique_lock<std::mutex> block_lck;

    int maxStage;
    int maxMap;
    int partitionNum;
    int stageId;
    int mapId;
    int partitionId;
    long size;
    char *data;
    int inflight;
    char* data_addr;
    PMPool* pmpool_ptr;

    Request(PMPool* pmpool_ptr,
            int maxStage,
            int maxMap,
            int partitionNum,
            int stageId, 
            int mapId, 
            int partitionId,
            long size,
            char* data);
    ~Request();
    long setPartition();
    long getResult();
};


static void taskset(int core_start, int core_end) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int cpu = core_start; cpu < core_end; cpu ++) {
        CPU_SET(cpu, &cpuset);
    }
    pthread_t thId = pthread_self();
    pthread_setaffinity_np(thId, sizeof(cpu_set_t), &cpuset);
}


PMPool::PMPool(const char* dev, int maxStage, int maxMap, int core_s, int core_e):
    maxStage(maxStage),
    maxMap(maxMap),
    core_s(core_s),
    core_e(core_e),
    stop(false),
    worker(&PMPool::process, this) {

  const char *pool_layout_name = "pmem_spark_shuffle";
	std::string prefix = "/dev/dax";
	std::string lock_file = "/tmp/spark_dax.lock";
	int fd = open(lock_file.c_str(), O_RDWR|O_CREAT);
	assert(fd != -1);
	lockf(fd, F_LOCK, 0);
	char* buf = new char[10];
    std::string next_dax;
    std::string dax = "0.0";
    int ret = pread(fd, buf, 2, 0);
    if (ret == 0) {
      buf = (char*)("0");
    }
    int idx = std::stoi(buf);
    dax = std::to_string(idx)+".0";
    next_dax = std::to_string(idx+1);
    ret = pwrite(fd, next_dax.c_str(), next_dax.length(), 0);
    device = prefix + dax;
    lockf(fd, F_ULOCK, 0);
    free(buf);
    close(fd);

    cout << "PMPOOL is " << device << endl;
    pmpool = pmemobj_open(device.c_str(), pool_layout_name);
    if (pmpool == NULL) {
        pmpool = pmemobj_create(device.c_str(), pool_layout_name, 0, S_IRUSR | S_IWUSR);
    }
    if (pmpool == NULL) {
        cerr << "Failed to open pool " << pmemobj_errormsg() << endl; 
        exit(-1);
    }

    stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
}

PMPool::~PMPool() {
    while(request_queue.size() > 0) {
        fprintf(stderr, "%s request queue size is %d\n", device.c_str(), request_queue.size());
        sleep(1);
    }
    fprintf(stderr, "%s request queue size is %d\n", device.c_str(), request_queue.size());
    stop = true;
    worker.join();
    pmemobj_close(pmpool);
}

void PMPool::process() {
    Request *cur_req;
    while(!stop) {
        cur_req = (Request*)request_queue.dequeue();
        if (cur_req != nullptr) {
            cur_req->setPartition();
        }
    }
}

long PMPool::setPartition(
        int partitionNum,
        int stageId, 
        int mapId, 
        int partitionId,
        long size,
        char* data ) {
    //fprintf(stderr, "%s request queue size is %d\n", device.c_str(), request_queue.size());
    Request write_request(this, maxStage, maxMap, partitionNum, stageId, mapId, partitionId, size, data);
    request_queue.enqueue((void*)&write_request);
    return write_request.getResult();
}

long PMPool::getPartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {

    //taskset(core_s, core_e); 
    if(TOID_IS_NULL(D_RO(stageArrayRoot)->stageArray)){
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: stageArray none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    TOID(struct StageArrayItem) stageArrayItem = D_RO(D_RO(stageArrayRoot)->stageArray)->items[stageId];
    if(TOID_IS_NULL(stageArrayItem) || TOID_IS_NULL(D_RO(stageArrayItem)->mapArray)) {
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: stageArrayItem OR mapArray none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    TOID(struct MapArray) mapArray = D_RO(stageArrayItem)->mapArray;
    TOID(struct MapArrayItem) mapArrayItem = D_RO(D_RO(stageArrayItem)->mapArray)->items[mapId];
    if(TOID_IS_NULL(mapArrayItem) || TOID_IS_NULL(D_RO(mapArrayItem)->partitionArray)){
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: mapArrayItem OR partitionArray none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    TOID(struct PartitionArrayItem) partitionArrayItem = D_RO(D_RO(mapArrayItem)->partitionArray)->items[partitionId];
    if(TOID_IS_NULL(partitionArrayItem) || TOID_IS_NULL(D_RO(partitionArrayItem)->first_block)) {
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: partitionArrayItem OR partitionBlock none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    long data_length = D_RO(partitionArrayItem)->partition_size;
    mb->buf = new char[data_length]();
    long off = 0;
    TOID(struct PartitionBlock) partitionBlock = D_RO(partitionArrayItem)->first_block;

    char* data_addr;
    while(!TOID_IS_NULL(partitionBlock)) {
        data_addr = (char*)pmemobj_direct(D_RO(partitionBlock)->data);
        //printf("getPartition data_addr: %p\n", data_addr);

        memcpy(mb->buf + off, data_addr, D_RO(partitionBlock)->data_size);
        off += D_RO(partitionBlock)->data_size;
        partitionBlock = D_RO(partitionBlock)->next_block;
    }

    return data_length;
}

Request::Request(PMPool* pmpool_ptr,
        int maxStage,
        int maxMap,
        int partitionNum,
        int stageId, 
        int mapId, 
        int partitionId,
        long size,
        char* data):
    pmpool_ptr(pmpool_ptr),
    maxStage(maxStage),
    maxMap(maxMap),
    partitionNum(partitionNum),
    stageId(stageId),
    mapId(mapId),
    partitionId(partitionId),
    size(size),
    data(data),
    data_addr(nullptr),
    lck(mtx), block_lck(block_mtx) {    
    inflight = 1;
}

Request::~Request() {
}

long Request::getResult() {
    if (inflight > 0){
        cv.wait(lck);
    }
    if (data_addr == nullptr)
      return -1;
    return (long)data_addr;
}
    
long Request::setPartition() {
    int ret = 0;
    bool should_lock = true;
    
    TX_BEGIN(pmpool_ptr->pmpool) {
        //taskset(core_s, core_e); 
        //cout << this << " enter setPartition tx" << endl;
        TX_ADD(pmpool_ptr->stageArrayRoot);
        // add a lock flag

        if (TOID_IS_NULL(D_RO(pmpool_ptr->stageArrayRoot)->stageArray)) {
            D_RW(pmpool_ptr->stageArrayRoot)->stageArray = TX_ZALLOC(struct StageArray, sizeof(struct StageArray) + maxStage * sizeof(struct StageArrayItem));
        }
        
        TX_ADD_FIELD(pmpool_ptr->stageArrayRoot, stageArray);
        TOID(struct StageArrayItem) *stageArrayItem = &(D_RW(D_RW(pmpool_ptr->stageArrayRoot)->stageArray)->items[stageId]);
        if (TOID_IS_NULL(*stageArrayItem)) {
            *stageArrayItem = TX_ZNEW(struct StageArrayItem);
        }
        TX_ADD(*stageArrayItem);
        if (TOID_IS_NULL(D_RO(*stageArrayItem)->mapArray)) {
            D_RW(*stageArrayItem)->mapArray = TX_ZALLOC(struct MapArray, sizeof(struct MapArray) + maxMap * sizeof(struct MapArrayItem));
        }

        TX_ADD_FIELD(*stageArrayItem, mapArray);
        TOID(struct MapArrayItem) *mapArrayItem = &(D_RW(D_RW(*stageArrayItem)->mapArray)->items[mapId]);
        if (TOID_IS_NULL(*mapArrayItem)) {
            *mapArrayItem = TX_ZNEW(struct MapArrayItem);
        }
        TX_ADD(*mapArrayItem);
        if (TOID_IS_NULL(D_RO(*mapArrayItem)->partitionArray)) {
            D_RW(*mapArrayItem)->partitionArray = TX_ZALLOC(struct PartitionArray, sizeof(struct PartitionArray) +  partitionNum * sizeof(struct PartitionArrayItem));
        }

        TX_ADD_FIELD(*mapArrayItem, partitionArray);
        TOID(struct PartitionArrayItem) *partitionArrayItem = &(D_RW(D_RW(*mapArrayItem)->partitionArray)->items[partitionId]);
        if (TOID_IS_NULL(*partitionArrayItem)) {
            *partitionArrayItem = TX_ZNEW(struct PartitionArrayItem);
        }
        TX_ADD(*partitionArrayItem);
        TX_ADD_FIELD(*partitionArrayItem, partition_size);
        D_RW(*partitionArrayItem)->partition_size += size;

        TOID(struct PartitionBlock) *partitionBlock = &(D_RW(*partitionArrayItem)->first_block);
        while(!TOID_IS_NULL(*partitionBlock)) {
            *partitionBlock = D_RW(*partitionBlock)->next_block;
        }

        TX_ADD_DIRECT(partitionBlock);
        *partitionBlock = TX_ZALLOC(struct PartitionBlock, 1);
        D_RW(*partitionBlock)->data = pmemobj_tx_zalloc(size, 0);
        
        D_RW(*partitionBlock)->data_size = size;
        D_RW(*partitionBlock)->next_block = TOID_NULL(struct PartitionBlock);

        data_addr = (char*)pmemobj_direct(D_RW(*partitionBlock)->data);
        //printf("setPartition data_addr: %p\n", data_addr);
        pmemobj_tx_add_range_direct((const void *)data_addr, size);

        memcpy(data_addr, data, size);
    } TX_ONCOMMIT {
        should_lock = false;
        block_cv.notify_all();
    } TX_ONABORT {
        fprintf(stderr, "set Partition of shuffle_%d_%d_%d failed. Error: %s\n", stageId, mapId, partitionId, pmemobj_errormsg());
        ret = -1;
        exit(-1);
    } TX_END
    if (should_lock){
        cout << this << " wait till commit" << endl;
        block_cv.wait(block_lck);
    }
    //cout << this << " leave setPartition tx" << endl;
    inflight--;
    cv.notify_all();
    if (data_addr == nullptr)
      return -1;
    return (long)data_addr;
}
