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
//POBJ_LAYOUT_ROOT(PersistentMemoryStruct, struct StageArrayRoot);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayRoot);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, TOID(struct StageArrayItem));
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct MapArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, TOID(struct MapArrayItem));
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, TOID(struct PartitionArrayItem));
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionBlock);
POBJ_LAYOUT_END(PersistentMemoryStruct);

struct StageArrayRoot {
    TOID_ARRAY(struct StageArrayItem) stageArray;
};

struct StageArrayItem {
    TOID_ARRAY(struct MapArrayItem) mapArray;
};

struct MapArrayItem {
    TOID_ARRAY(struct PartitionArrayItem) partitionArray;
};

struct PartitionArrayItem {
    TOID(struct PartitionBlock) first_block;
    long partition_size;
};

struct PartitionBlock {
    TOID(struct PartitionBlock) next_block;
    long data_size;
    char data[];
};

struct MemoryBlock {
    char* buf;
    MemoryBlock() {
    }

    ~MemoryBlock() {
        delete buf;
    }
};

struct PartitionBlockArg {
    long size;
    char* data;
    PartitionBlockArg(long size, char* data): size(size), data(data){}
};

static int initPartitionBlock(pmemobjpool *pmpool, void *ptr, void *args) {
    struct PartitionBlock *partitionBlock = (struct PartitionBlock*)ptr;
    partitionBlock->data_size = ((struct PartitionBlockArg*)args)->size;
    //pmemobj_persist(pmpool, &(partitionBlock->data_size), sizeof(long));

    memcpy(partitionBlock->data, ((struct PartitionBlockArg*)args)->data, ((struct PartitionBlockArg*)args)->size);
    partitionBlock->next_block = TOID_NULL(struct PartitionBlock);
    return 0;
}

static int initStageArrayItem(pmemobjpool *pmpool, void *ptr, void *arg) {
    //pmemobj_persist(pmpool, ptr, *((int*)arg));
    return 0;
}

static int initMapArrayItem(pmemobjpool *pmpool, void *ptr, void *arg) {
    //pmemobj_persist(pmpool, ptr, *((int*)arg));
    return 0;
}

static int initPartitionArrayItem(pmemobjpool *pmpool, void *ptr, void *arg) {
    //pmemobj_persist(pmpool, ptr, *((int*)arg));
    return 0;
}

class PMPool {
public:
    PMPool(const char* dev, int maxStage, int maxMap):
        maxStage(maxStage),
        maxMap(maxMap) {
        
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
        if (TOID_IS_NULL(D_RO(stageArrayRoot)->stageArray)) {
            allocate_array(maxStage, &(D_RW(stageArrayRoot)->stageArray));
        }
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
        TOID_ARRAY(struct StageArrayItem) *stageArray = &(D_RW(stageArrayRoot)->stageArray);
        assert(!TOID_IS_NULL(D_RO(stageArrayRoot)->stageArray));

        TOID(struct StageArrayItem) *stageArrayItem = &(D_RW(*stageArray)[stageId]);
        tmp = D_RW(*stageArray);

        if (TOID_IS_NULL(D_RO(*stageArrayItem)->mapArray)) {
            allocate_array(maxMap, &(D_RW(*stageArrayItem)->mapArray));
        }
        tmp = D_RW(*stageArray);

        TOID_ARRAY(struct MapArrayItem) *mapArray = &(D_RW(*stageArrayItem)->mapArray);
        TOID(struct MapArrayItem) *mapArrayItem = &(D_RW(*mapArray)[mapId]);
    
        if (TOID_IS_NULL(D_RO(*mapArrayItem)->partitionArray)) {
            allocate_array(partitionNum, &(D_RW(*mapArrayItem)->partitionArray));
        }

        tmp = D_RW(*stageArray);

        TOID_ARRAY(struct PartitionArrayItem) *partitionArray = &(D_RW(*mapArrayItem)->partitionArray);
        TOID(struct PartitionArrayItem) *partitionArrayItem = &(D_RW(*partitionArray)[partitionId]);

        struct PartitionBlockArg args(size, data);
        TOID(struct PartitionBlock) *partitionBlock = &(D_RW(*partitionArrayItem)->first_block);
        while(!TOID_IS_NULL(*partitionBlock)) {
					  // loop to find the last partitionBlock
            partitionBlock = &(D_RW(*partitionBlock)->next_block);
        }
        tmp = D_RW(*stageArray);
        POBJ_ALLOC(pmpool, partitionBlock, struct PartitionBlock, sizeof(struct PartitionBlock) + size - 1, initPartitionBlock, &args);
        long updated_size = D_RO(*partitionArrayItem)->partition_size + size;
        //pmemobj_memcpy_persist(pmpool, &(D_RW(*partitionArrayItem)->partition_size), &updated_size, sizeof(long));
        memcpy(&(D_RW(*partitionArrayItem)->partition_size), &updated_size, sizeof(long));
        tmp = D_RW(*stageArray);
    
        return ret;
    }

    long getPartition(
            MemoryBlock* mb,
            int stageId,
            int mapId,
            int partitionId ) {

        stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
        //TOID(struct StageArrayRoot) stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
        assert(!TOID_IS_NULL(D_RO(stageArrayRoot)->stageArray));

        TOID(struct StageArrayItem) stageArrayItem = D_RO(D_RO(stageArrayRoot)->stageArray)[stageId];
        assert(!TOID_IS_NULL(D_RO(stageArrayItem)->mapArray));

        TOID(struct MapArrayItem) mapArrayItem = D_RO(D_RO(stageArrayItem)->mapArray)[mapId];
        assert(!TOID_IS_NULL(D_RO(mapArrayItem)->partitionArray));
    
        TOID(struct PartitionArrayItem) partitionArrayItem = D_RO(D_RO(mapArrayItem)->partitionArray)[partitionId];
        assert(!TOID_IS_NULL(D_RO(partitionArrayItem)->first_block));

        long data_length = D_RO(partitionArrayItem)->partition_size;
        mb->buf = new char[data_length]();
        long off = 0;
        TOID(struct PartitionBlock) partitionBlock = D_RO(partitionArrayItem)->first_block;

        while(!TOID_IS_NULL(partitionBlock)) {
            memcpy(mb->buf + off, D_RO(partitionBlock)->data, D_RO(partitionBlock)->data_size);
            off += D_RO(partitionBlock)->data_size;
            partitionBlock = D_RO(partitionBlock)->next_block;
        }

        return data_length;
    }

private:
    PMEMobjpool *pmpool;
    TOID(struct StageArrayRoot) stageArrayRoot;
    TOID(struct StageArrayItem) *tmp;
    int maxStage;
    int maxMap;
    uint64_t poolId;
    int inflight = 0;
    std::mutex mtx;
    std::condition_variable cv;

    void allocate_array(int size, TOID_ARRAY(struct StageArrayItem) *array) {
        int arg_size = sizeof(TOID(struct StageArrayItem)) * size;
        POBJ_ZALLOC(pmpool, array, TOID(struct StageArrayItem), arg_size);

	      if (TOID_IS_NULL(*array)) {
		        fprintf(stderr, "Unable to allocate Stage array, size is %d\n", arg_size);
            exit(-1);
	      }

	      for (int i = 0; i < size; i++) {
		        POBJ_ZNEW(pmpool, &D_RW(*array)[i], struct StageArrayItem);
	      }
    }

    void allocate_array(int size, TOID_ARRAY(struct MapArrayItem) *array) {
        int arg_size = sizeof(TOID(struct MapArrayItem)) * size;
        POBJ_ZALLOC(pmpool, array, TOID(struct MapArrayItem), arg_size);

	      if (TOID_IS_NULL(*array)) {
		        fprintf(stderr, "Unable to allocate Map array, size is %d\n", arg_size);
            exit(-1);
	      }

	      for (int i = 0; i < size; i++) {
		        POBJ_ZNEW(pmpool, &D_RW(*array)[i], struct MapArrayItem);
	      }
    }

    void allocate_array(int size, TOID_ARRAY(struct PartitionArrayItem) *array) {
        int arg_size = sizeof(TOID(struct PartitionArrayItem)) * size;
        POBJ_ZALLOC(pmpool, array, TOID(struct PartitionArrayItem), arg_size);

	      if (TOID_IS_NULL(*array)) {
		        fprintf(stderr, "Unable to allocate Shuffle array, size is %d\n", arg_size);
            exit(-1);
	      }

	      for (int i = 0; i < size; i++) {
		        POBJ_ZNEW(pmpool, &D_RW(*array)[i], struct PartitionArrayItem);
	      }
    }
};


