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
    int len;
    MemoryBlock() {
    }

    ~MemoryBlock() {
        delete[] buf;
    }
};

struct BlockInfo {
    long* data;
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
      processed(false),
      committed(false),
      lck(mtx), block_lck(block_mtx) {
      }
    ~Request(){}
    virtual void exec() = 0;
    virtual long getResult() = 0;
protected:
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

    size_t key;
    PMPool<string>* pmpool_ptr;

    TOID(struct Block) *getAndCreateBlock();
    TOID(struct Block) getBlock();
    void freeBlock(TOID(struct Block) block);
};

class WriteRequest : Request {
public:
    char* data_addr; //Pmem Block Addr
    WriteRequest(PMPool<string>* pmpool_ptr, size_t key, long size, char* data, bool set_clean):
    Request(pmpool_ptr, key),
    size(size),
    data(data),
    data_addr(nullptr),
    set_clean(set_clean){
    }
    ~WriteRequest(){}
    void exec();
    long getResult();
private:
    long size;
    char *data;
    bool set_clean;
    void setBlock();
};

class ReadRequest : Request {
public:
    ReadRequest(PMPool<string>* pmpool_ptr, MemoryBlock *mb, size_t key):
    Request(pmpool_ptr, key), mb(mb){
    }
    ~ReadRequest(){}
    void exec();
    long getResult();
private:
    MemoryBlock *mb;
    long data_length = -1;
    void readBlock();
};

class MetaRequest : Request {
public:
    MetaRequest(PMPool<string>* pmpool_ptr, BlockInfo* block_info, size_t key):
    Request(pmpool_ptr, key), block_info(block_info){
    }
    ~MetaRequest(){}
    void exec();
    long getResult();
private:
    BlockInfo* block_info;
    long array_length = -1;
    void getBlockIndex();
};

class SizeRequest: Request {
public:
    SizeRequest(PMPool<string>* pmpool_ptr, size_t key):
    Request(pmpool_ptr, key){
    }
    ~SizeRequest(){}
    void exec();
    long getResult();
private:
    long data_length = -1;
    void getBlockSize();
};

class DeleteRequest: Request {
public:
    DeleteRequest(PMPool<string>* pmpool_ptr, size_t key):
    Request(pmpool_ptr, key){
    }
    ~DeleteRequest(){}
    void exec();
    long getResult();
private:
    long ret = -1;
    void deleteBlock();
};

#endif
