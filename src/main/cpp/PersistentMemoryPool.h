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

static void taskset(int core_start, int core_end) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int cpu = core_start; cpu < core_end; cpu ++) {
        CPU_SET(cpu, &cpuset);
    }
    pthread_t thId = pthread_self();
    pthread_setaffinity_np(thId, sizeof(cpu_set_t), &cpuset);
}

class PMPool {
private:
    PMEMobjpool *pmpool;
    TOID(struct StageArrayRoot) stageArrayRoot;
    int maxStage;
    int maxMap;
    std::mutex mtx;
    std::condition_variable cv;
    int core_s;
    int core_e;
public:
    PMPool(const char* dev, int maxStage, int maxMap, int core_s, int core_e):
        maxStage(maxStage),
        maxMap(maxMap),
        core_s(core_s),
        core_e(core_e) {
        
        const char *pool_layout_name = "pmem_spark_shuffle";
        pmpool = pmemobj_open(dev, pool_layout_name);
        if (pmpool == NULL) {
            pmpool = pmemobj_create(dev, pool_layout_name, 0, S_IRUSR | S_IWUSR);
        }
        if (pmpool == NULL) {
            cerr << "Failed to open pool " << pmemobj_errormsg() << endl; 
            exit(-1);
        }

        stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
    }

    ~PMPool() {
        pmemobj_close(pmpool);
    }

    int setPartition(
            int partitionNum,
            int stageId, 
            int mapId, 
            int partitionId,
            long size,
            char* data ) {
        int ret = 0;
        int inflight = 1;
        std::unique_lock<std::mutex> lck(mtx);
        TX_BEGIN(pmpool) {
            taskset(core_s, core_e); 
            TX_ADD(stageArrayRoot);
            if (TOID_IS_NULL(D_RO(stageArrayRoot)->stageArray)) {
                D_RW(stageArrayRoot)->stageArray = TX_ZALLOC(struct StageArray, sizeof(struct StageArray) + maxStage * sizeof(struct StageArrayItem));
            }
            
            TX_ADD_FIELD(stageArrayRoot, stageArray);
            TOID(struct StageArrayItem) *stageArrayItem = &(D_RW(D_RW(stageArrayRoot)->stageArray)->items[stageId]);
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

            char* data_addr = (char*)pmemobj_direct(D_RW(*partitionBlock)->data);
            //printf("setPartition data_addr: %p\n", data_addr);
            pmemobj_tx_add_range_direct((const void *)data_addr, size);
            memcpy(data_addr, data, size);
        } TX_ONCOMMIT {
            inflight--;
            cv.notify_all();
        } TX_ONABORT {
            fprintf(stderr, "set Partition of shuffle_%d_%d_%d failed. Error: %s\n", stageId, mapId, partitionId, pmemobj_errormsg());
            ret = -1;
            exit(-1);
        } TX_END
        while (inflight > 0) cv.wait(lck);
        return ret;
    }

    long getPartition(
            MemoryBlock* mb,
            int stageId,
            int mapId,
            int partitionId ) {

        taskset(core_s, core_e); 
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

};
