#include "PersistentMemoryPool.h"
#include <string>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <fstream>

PMPool::PMPool(const char* dev, int maxStage, int maxMap, long size):
    maxStage(maxStage),
    maxMap(maxMap),
    dev(dev) {

  const char *pool_layout_name = "pmem_spark_shuffle";
  // if this is a fsdax device
  // we need to create 
  // if this is a devdax device

  pmpool = pmemobj_open(dev, pool_layout_name);
  if (pmpool == NULL) {
      pmpool = pmemobj_create(dev, pool_layout_name, size, S_IRUSR | S_IWUSR);
  }
  if (pmpool == NULL) {
      cerr << "Failed to create pool, kill process, errmsg: devname is " << dev << ", size is " << size << ", " << pmemobj_errormsg() << endl; 
      exit(-1);
  }

  stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
}

PMPool::~PMPool() {
    pmemobj_close(pmpool);
}

long PMPool::getRootAddr() {
    return (long)pmpool;
}

long PMPool::setMapPartition(
        int partitionNum,
        int stageId, 
        int mapId, 
        int partitionId,
        long size,
        char* data,
        bool clean,
        int numMaps) {
    WriteRequest write_request(this, maxStage, numMaps, partitionNum, stageId, 0, mapId, partitionId, size, data, clean);
		std::lock_guard<std::mutex> lk(mtx);
    write_request.exec();
    return write_request.getResult();
}

long PMPool::setReducePartition(
        int partitionNum,
        int stageId, 
        int partitionId,
        long size,
        char* data,
        bool clean,
        int numMaps) {
    WriteRequest write_request(this, maxStage, 1, partitionNum, stageId, 1, 0, partitionId, size, data, clean);
		std::lock_guard<std::mutex> lk(mtx);
		write_request.exec();
    return write_request.getResult();
}

long PMPool::getMapPartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {
    ReadRequest read_request(this, mb, stageId, 0, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
    read_request.exec();
    return read_request.getResult();
}
    
long PMPool::getReducePartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {
    ReadRequest read_request(this, mb, stageId, 1, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
    read_request.exec();
    read_request.getResult();
    return 0;
}

long PMPool::getMapPartitionBlockInfo(BlockInfo *blockInfo, int stageId, int mapId, int partitionId) {
    MetaRequest meta_request(this, blockInfo, stageId, 0, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
    meta_request.exec();
    return meta_request.getResult();
}

long PMPool::getReducePartitionBlockInfo(BlockInfo *blockInfo, int stageId, int mapId, int partitionId) {
    MetaRequest meta_request(this, blockInfo, stageId, 1, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
    meta_request.exec();
    return meta_request.getResult();
}

long PMPool::getMapPartitionSize(int stageId, int mapId, int partitionId) {
    SizeRequest size_request(this, stageId, 0, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
    size_request.exec();
    return size_request.getResult();
}

long PMPool::getReducePartitionSize(int stageId, int mapId, int partitionId) {
    SizeRequest size_request(this, stageId, 1, mapId, partitionId);
    size_request.exec();
    return size_request.getResult();
}

long PMPool::deleteMapPartition(int stageId, int mapId, int partitionId) {
    DeleteRequest delete_request(this, stageId, 0, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
		delete_request.exec();
    return delete_request.getResult();
}

long PMPool::deleteReducePartition(int stageId, int mapId, int partitionId) {
    DeleteRequest delete_request(this, stageId, 1, mapId, partitionId);
		std::lock_guard<std::mutex> lk(mtx);
		delete_request.exec();
    return delete_request.getResult();
}

