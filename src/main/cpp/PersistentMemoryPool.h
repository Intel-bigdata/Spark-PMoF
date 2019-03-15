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
#define TYPENUM 2

POBJ_LAYOUT_BEGIN(PersistentMemoryStruct);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayRoot);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct TypeArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct TypeArrayItem);
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
    TOID(struct TypeArray) typeArray;
};

struct TypeArray {
    int size;
    TOID(struct TypeArrayItem) items[];
};

struct TypeArrayItem {
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
    TOID(struct PartitionBlock) last_block;
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
    const char* dev;
    WorkQueue<void*> request_queue;
    PMPool(const char* dev, int maxStage, int maxMap, long size);
    ~PMPool();
    long getRootAddr();
    long setMapPartition(int partitionNum, int stageId, int mapId, int partitionId, long size, char* data, bool clean);
    long setReducePartition(int partitionNum, int stageId, int partitionId, long size, char* data, bool clean);
    long getMapPartition(MemoryBlock* mb, int stageId, int mapId, int partitionId);
    long getReducePartition(MemoryBlock* mb, int stageId, int mapId, int partitionId);
    long getPartition(MemoryBlock* mb, int stageId, int typeId, int mapId, int partitionId);
    void process();
};

class Request {
public:
    // add lock to make this request blocked
    std::mutex mtx;
    std::condition_variable cv;
    bool processed;
    std::unique_lock<std::mutex> lck;

    // add lock to make func blocked
    std::mutex block_mtx;
    std::condition_variable block_cv;
    bool committed;
    std::unique_lock<std::mutex> block_lck;

    int maxStage;
    int maxMap;
    int partitionNum;
    int stageId;
    int typeId;
    int mapId;
    int partitionId;
    long size;
    char *data;
    bool set_clean;
    char* data_addr;
    PMPool* pmpool_ptr;

    Request(PMPool* pmpool_ptr,
            int maxStage,
            int maxMap,
            int partitionNum,
            int stageId, 
            int typeId,
            int mapId, 
            int partitionId,
            long size,
            char* data,
            bool set_clean);
    ~Request();
    void setPartition();
    long getResult();
};

PMPool::PMPool(const char* dev, int maxStage, int maxMap, long size):
    maxStage(maxStage),
    maxMap(maxMap),
    stop(false),
    dev(dev),
    worker(&PMPool::process, this) {

  const char *pool_layout_name = "pmem_spark_shuffle";
  cout << "PMPOOL is " << dev << endl;
  // if this is a fsdax device
  // we need to create 
  // if this is a devdax device

  pmpool = pmemobj_open(dev, pool_layout_name);
  if (pmpool == NULL) {
      cout << "Failed to open dev, try to create, errmsg: " << pmemobj_errormsg() << endl; 
      pmpool = pmemobj_create(dev, pool_layout_name, size, S_IRUSR | S_IWUSR);
  }
  if (pmpool == NULL) {
      cerr << "Failed to create pool, kill process, errmsg: " << pmemobj_errormsg() << endl; 
      exit(-1);
  }

  stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
}

PMPool::~PMPool() {
    while(request_queue.size() > 0) {
        fprintf(stderr, "%s request queue size is %d\n", dev, request_queue.size());
        sleep(1);
    }
    fprintf(stderr, "%s request queue size is %d\n", dev, request_queue.size());
    stop = true;
    worker.join();
    pmemobj_close(pmpool);
}

long PMPool::getRootAddr() {
    return (long)pmpool;
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

long PMPool::setMapPartition(
        int partitionNum,
        int stageId, 
        int mapId, 
        int partitionId,
        long size,
        char* data,
        bool clean) {
    Request write_request(this, maxStage, maxMap, partitionNum, stageId, 0, mapId, partitionId, size, data, clean);
    request_queue.enqueue((void*)&write_request);
    return write_request.getResult();
}

long PMPool::setReducePartition(
        int partitionNum,
        int stageId, 
        int partitionId,
        long size,
        char* data,
        bool clean) {
    Request write_request(this, maxStage, 1, partitionNum, stageId, 1, 0, partitionId, size, data, clean);
    request_queue.enqueue((void*)&write_request);
    return write_request.getResult();
}

long PMPool::getMapPartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {
  return getPartition(mb, stageId, 0, mapId, partitionId);
}

long PMPool::getReducePartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {
  return getPartition(mb, stageId, 1, mapId, partitionId);
}

long PMPool::getPartition(
        MemoryBlock* mb,
        int stageId,
        int typeId,
        int mapId,
        int partitionId ) {
    //taskset(core_s, core_e); 
    if(TOID_IS_NULL(D_RO(stageArrayRoot)->stageArray)){
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: stageArray none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    TOID(struct StageArrayItem) stageArrayItem = D_RO(D_RO(stageArrayRoot)->stageArray)->items[stageId];
    if(TOID_IS_NULL(stageArrayItem) || TOID_IS_NULL(D_RO(stageArrayItem)->typeArray)) {
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: stageArrayItem OR typeArray none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    TOID(struct TypeArray) typeArray = D_RO(stageArrayItem)->typeArray;
    TOID(struct TypeArrayItem) typeArrayItem = D_RO(D_RO(stageArrayItem)->typeArray)->items[typeId];
    if(TOID_IS_NULL(typeArrayItem) || TOID_IS_NULL(D_RO(typeArrayItem)->mapArray)) {
        fprintf(stderr, "get Partition of shuffle_%d_%d_%d failed: typeArrayItem OR mapArray none Exists.\n", stageId, mapId, partitionId);
        return -1;
    }

    TOID(struct MapArray) mapArray = D_RO(typeArrayItem)->mapArray;
    TOID(struct MapArrayItem) mapArrayItem = D_RO(D_RO(typeArrayItem)->mapArray)->items[mapId];
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

    //printf("getPartition length is %d\n", data_length);
    return data_length;
}

Request::Request(PMPool* pmpool_ptr,
        int maxStage,
        int maxMap,
        int partitionNum,
        int stageId, 
        int typeId, 
        int mapId, 
        int partitionId,
        long size,
        char* data,
        bool set_clean):
    pmpool_ptr(pmpool_ptr),
    maxStage(maxStage),
    maxMap(maxMap),
    partitionNum(partitionNum),
    stageId(stageId),
    typeId(typeId),
    mapId(mapId),
    partitionId(partitionId),
    size(size),
    data(data),
    data_addr(nullptr),
    set_clean(set_clean),
    processed(false),
    committed(false),
    lck(mtx), block_lck(block_mtx) {
}

Request::~Request() {
}

long Request::getResult() {
    {
        while (!processed) {
            usleep(5);
        }
        //cv.wait(lck, [&]{return processed;});
    }
    //fprintf(stderr, "get Result for %d_%d_%d\n", stageId, mapId, partitionId);
    if (data_addr == nullptr)
      return -1;
    return (long)data_addr;
}
    
void Request::setPartition() {
    TX_BEGIN(pmpool_ptr->pmpool) {
        //taskset(core_s, core_e); 
        //cout << this << " enter setPartition tx" << endl;
        //fprintf(stderr, "request for %d_%d_%d\n", stageId, mapId, partitionId);
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
        if (TOID_IS_NULL(D_RO(*stageArrayItem)->typeArray)) {
            D_RW(*stageArrayItem)->typeArray = TX_ZALLOC(struct TypeArray, sizeof(struct TypeArray) + TYPENUM * sizeof(struct TypeArrayItem));
        }

        TX_ADD_FIELD(*stageArrayItem, typeArray);
        TOID(struct TypeArrayItem) *typeArrayItem = &(D_RW(D_RW(*stageArrayItem)->typeArray)->items[typeId]);
        if (TOID_IS_NULL(*typeArrayItem)) {
            *typeArrayItem = TX_ZNEW(struct TypeArrayItem);
        }
        TX_ADD(*typeArrayItem);
        if (TOID_IS_NULL(D_RO(*typeArrayItem)->mapArray)) {
            D_RW(*typeArrayItem)->mapArray = TX_ZALLOC(struct MapArray, sizeof(struct MapArray) + maxMap * sizeof(struct MapArrayItem));
        }

        TX_ADD_FIELD(*typeArrayItem, mapArray);
        TOID(struct MapArrayItem) *mapArrayItem = &(D_RW(D_RW(*typeArrayItem)->mapArray)->items[mapId]);
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

        TOID(struct PartitionBlock) *partitionBlock = &(D_RW(*partitionArrayItem)->first_block);
        if (set_clean == false) {
            // jump to last block
            D_RW(*partitionArrayItem)->partition_size += size;
            *partitionBlock = D_RW(*partitionArrayItem)->last_block;
            /*while(!TOID_IS_NULL(*partitionBlock)) {
                *partitionBlock = D_RW(*partitionBlock)->next_block;
            }*/
        } else {
            //TODO: we should remove unused blocks
            D_RW(*partitionArrayItem)->partition_size = size;
        }

        TX_ADD_DIRECT(partitionBlock);
        *partitionBlock = TX_ZALLOC(struct PartitionBlock, 1);
        D_RW(*partitionBlock)->data = pmemobj_tx_zalloc(size, 0);
        
        D_RW(*partitionBlock)->data_size = size;
        D_RW(*partitionBlock)->next_block = TOID_NULL(struct PartitionBlock);
        D_RW(*partitionArrayItem)->last_block = *partitionBlock;

        data_addr = (char*)pmemobj_direct(D_RW(*partitionBlock)->data);
        //printf("setPartition data_addr: %p\n", data_addr);
        pmemobj_tx_add_range_direct((const void *)data_addr, size);

        memcpy(data_addr, data, size);
    } TX_ONCOMMIT {
        committed = true;
        block_cv.notify_all();
    } TX_ONABORT {
        fprintf(stderr, "set Partition of %d_%d_%d failed, type is %d, partitionNum is %d. Error: %s\n", stageId, mapId, partitionId, typeId, partitionNum, pmemobj_errormsg());
        exit(-1);
    } TX_END

    block_cv.wait(block_lck, [&]{return committed;});
    //fprintf(stderr, "request committed %d_%d_%d\n", stageId, mapId, partitionId);

    processed = true;
    //cv.notify_all();
}
