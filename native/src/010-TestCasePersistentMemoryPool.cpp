#define CATCH_CONFIG_MAIN

#include "catch.hpp"
#include "PersistentMemoryPool.h"
#include "PmemBuffer.h"

//#define LENGTH 262144 /*256KB*/
#define LENGTH 50

TEST_CASE( "PmemBuffer operations", "[PmemBuffer]" ) {
  char data[LENGTH] = {};
  memset(data, 'a', LENGTH);

  PmemBuffer buf;

  SECTION( "write 256KB data to PmemBuffer" ) {
    buf.write(data, LENGTH);
    REQUIRE(buf.getRemaining() == LENGTH);
  }

  SECTION( "read 256KB data FROM PmemBuffer" ) {
    buf.write(data, LENGTH);
    char ret_data[LENGTH] = {};
    int size = buf.read(ret_data, LENGTH);
    REQUIRE(buf.getRemaining() == 0);
    REQUIRE(size == LENGTH);
  }

  SECTION( "read data exceeds remaining data size in PmemBuffer" ) {
    buf.write(data, LENGTH);
    char ret_data[LENGTH * 2] = {};
    int size = buf.read(ret_data, LENGTH * 2);
    REQUIRE(buf.getRemaining() == 0);
    REQUIRE(size == LENGTH);
  }

  SECTION( "do getDataForFlush twice check if only get same data once" ) {
    for (char c = 'a'; c < 'f'; c++) {
      memset(data + (c - 'a') * 3, c, 3);
    }
    /*data should be "aaabbbcccdddeee"*/
    buf.write(data, 15);

    char* firstTime = buf.getDataForFlush(buf.getRemaining());
    if (firstTime != nullptr) {
      firstTime[15] = 0;
      REQUIRE(strcmp(firstTime, "aaabbbcccdddeee") == 0);
    }

    char* secondTime = buf.getDataForFlush(buf.getRemaining());
    REQUIRE(secondTime == nullptr);
  }
}

TEST_CASE( "PersistentMemoryPool operations", "[PersistentMemory]" ) {
  PMPool pmpool("/dev/dax0.0", 100, 100, 0/*devdax size should be 0*/);

  char data[LENGTH] = {};
  memset(data, 'a', LENGTH);

  SECTION( "write data to PersistentMemory" ) {
    long addr = pmpool.setMapPartition(1000, 1, 0, 0, LENGTH, data, true, 100);
    long size = pmpool.getMapPartitionSize(1, 0, 0);
    REQUIRE(size == LENGTH);
  }

  SECTION( "read data from PersistentMemory" ) {
    MemoryBlock mb;
    long addr = pmpool.getMapPartition(&mb, 1, 0, 0);
    REQUIRE(mb.buf != nullptr);
    mb.buf[LENGTH] = 0;
    REQUIRE(strcmp(mb.buf, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") == 0);
  }

  SECTION( "get existing partition info from PersistentMemory" ) {
    BlockInfo bi;
    pmpool.getMapPartitionBlockInfo(&bi, 1, 0, 0);
    REQUIRE(bi.data != nullptr);
    REQUIRE(bi.data[1] == LENGTH);
  }

  SECTION( "get existing partition info with two blocks from PersistentMemory" ) {
    long addr_1 = pmpool.setMapPartition(1000, 2, 0, 0, LENGTH, data, true, 100);
    long addr_2 = pmpool.setMapPartition(1000, 2, 0, 0, LENGTH, data, false, 100);
    BlockInfo bi;
    pmpool.getMapPartitionBlockInfo(&bi, 2, 0, 0);
    REQUIRE(bi.data != nullptr);
    REQUIRE(bi.data[0] == addr_1);
    REQUIRE(bi.data[1] == LENGTH);
    REQUIRE(bi.data[2] == addr_2);
    REQUIRE(bi.data[3] == LENGTH);
  }
}
