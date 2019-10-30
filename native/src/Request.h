#ifndef PMPOOL_REQUEST_H
#define PMPOOL_REQUEST_H

#include <mutex>
#include <condition_variable>
#include <libpmemobj.h>
#include <cstdlib>

#define BLOCK_BUCKETS_NUM 1048576
#define BLOCKS_NUM 1024
using namespace std;

POBJ_LAYOUT_BEGIN(PersistentMemoryStruct);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct HashMapRoot);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct BlockBuckets);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct BlockBucketItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct BlockBucket);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct Block);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct BlockPartition);
POBJ_LAYOUT_END(PersistentMemoryStruct);

struct HashMapRoot {
    TOID(struct BlockBuckets) blockBuckets;
};

struct BlockBuckets {
    //size of BLOCK_BUCKETS_NUM
    int size;
    TOID(struct BlockBucketItem) items[];
};

struct BlockBucketItem {
    TOID(struct BlockBucket) blockBucket;
};

struct BlockBucket {
    //size of BLOCKS_NUM
    int size;
    TOID(struct Block) items[];
};

struct Block {
    size_t key;
    size_t size;
    int numBlocks;
    TOID(struct BlockPartition) firstBlockPartition;
    TOID(struct BlockPartition) lastBlockPartition;
    TOID(struct Block) nextBlock; //if collision happened
};

struct BlockPartition {
    TOID(struct BlockPartition) nextBlockPartition;
    long data_size;
    PMEMoid data;
};

struct MemoryBlock {
    char* buf;
    long len;
    MemoryBlock() {
    }

    ~MemoryBlock() {
        delete[] buf;
    }
};

struct BlockInfo {
    long* data;
    long len;
    BlockInfo() {
    }

    ~BlockInfo() {
        delete data;
    }
};

template <typename T> class PMPool;
class Request {
public:
    Request(PMPool<string>* pmpool_ptr, size_t key):
      pmpool_ptr(pmpool_ptr),
      key(key),
      committed(false) {}
    virtual ~Request() {}
    virtual void exec() {}
protected:
    PMPool<string>* pmpool_ptr;
    size_t key;
    bool committed;

    TOID(struct Block) *getAndCreateBlock();
    TOID(struct Block) getBlock();
    void freeBlock(TOID(struct Block) block);
};

class WriteRequest : Request {
public:
    WriteRequest(PMPool<string>* pmpool_ptr, size_t key, long size, char* data, bool set_clean):
    Request(pmpool_ptr, key),
    size(size),
    data(data),
    data_addr(nullptr),
    set_clean(set_clean){
    }
    ~WriteRequest(){}
    void exec();
private:
    long size;
    char *data;
    char* data_addr; //Pmem Block Addr
    bool set_clean;
    void setBlock();
};

class ReadRequest : Request {
public:
    ReadRequest(PMPool<string>* pmpool_ptr, MemoryBlock *mb, size_t key):
    Request(pmpool_ptr, key), mb(mb){}
    ~ReadRequest(){}
    void exec();
private:
    MemoryBlock *mb;
    void readBlock();
};

class MetaRequest : Request {
public:
    MetaRequest(PMPool<string>* pmpool_ptr, BlockInfo* block_info, size_t key):
    Request(pmpool_ptr, key), block_info(block_info){}
    ~MetaRequest(){}
    void exec();
private:
    BlockInfo* block_info;
    void getBlockIndex();
};

class SizeRequest: Request {
public:
    SizeRequest(PMPool<string>* pmpool_ptr, long* data_length, size_t key):
    Request(pmpool_ptr, key), data_length(data_length) {}
    ~SizeRequest(){}
    void exec();
private:
    long* data_length;
    void getBlockSize();
};

class DeleteRequest: Request {
public:
    DeleteRequest(PMPool<string>* pmpool_ptr, size_t key):
    Request(pmpool_ptr, key){}
    ~DeleteRequest(){}
    void exec();
private:
    void deleteBlock();
};

#endif
