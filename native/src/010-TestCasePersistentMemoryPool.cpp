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

TEST_CASE("PersistentMemoryPool operations", "[PersistentMemory]") {
  PMPool<string> pmpool("/dev/dax0.0", 0);
  char data[LENGTH] = {};
  memset(data, 'a', LENGTH);
  MemoryBlock block;
  pmpool.setBlock("a", LENGTH, data, true);
  pmpool.getBlock(&block, "a");
  REQUIRE(block.len == LENGTH);
  REQUIRE(pmpool.getBlockSize("a") == LENGTH);
}
