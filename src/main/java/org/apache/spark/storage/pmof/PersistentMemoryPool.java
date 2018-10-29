package org.apache.spark.storage.pmof;

import lib.llpl.*;
import static java.lang.Math.toIntExact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/* This class is used to make PM as a huge pool
 * All shuffle files will be stored in the same pool
 * Default Maximun shuffle file will set to 1000
 * Then follows shuffle block array
 *
 * Data Structure
 * =============================================
 * stage_id: 0        shuffle_array[0].address
 * stage_id: 1        shuffle_array[1].address
 * ... ...
 * stage_id: 1000     shuffle_array[999].address
 * ===shuffle_array[0]===
 * map_id: 0      block_array[0].address
 * map_id: 1      block_array[1].address
 * ... ...
 * map_id: 1000   block_array[999].address
 * block_array[0]
 * block_array[1]
 * ... ...
 * ===shuffle_array[1]===
 * ===shuffle_array[2]===
 * ... ...
 * =============================================
 * */

public class PersistentMemoryPool {
    private static final Logger logger = LoggerFactory.getLogger(PersistentMemoryPool.class);
    static final int HEADER_SIZE = 4;
    static final long DEFAULT_PMPOOL_SIZE = 26843545600L;
    private Heap pmHeap = null;
    private String path;
    int max_stages_num;
    int max_shuffles_num;

    MemoryBlock<Transactional> stageArray = null;
    
    PersistentMemoryPool(
        String path,
        int max_stages_num,
        int max_shuffles_num,
        long pool_size) {
      this.max_stages_num = max_stages_num;
      this.max_shuffles_num = max_shuffles_num;
      this.path = path;

      if (this.pmHeap == null) {
        logger.info("Open pmpool [" + path + "]");
        pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
        this.pmHeap = Heap.getHeap(path, pool_size);
      }

      long rootAddr = this.pmHeap.getRoot();
      if (rootAddr != 0) {
        logger.info("[" + path + "] Exists old shuffle array info.");
        this.stageArray = this.pmHeap.memoryBlockFromAddress(Transactional.class, rootAddr);
      } else {
        logger.info("[" + path + "] create new shuffle array info.");

        this.stageArray = this.pmHeap.allocateMemoryBlock(Transactional.class, Long.BYTES * max_stages_num);
        this.pmHeap.setRoot(stageArray.address());
      }
    }

    public void openShuffleBlock(int stageId, int shuffleId, int partitionNum) {
      MemoryBlock<Transactional> shuffleArray = null;
      MemoryBlock<Transactional> shuffleBlock = null;
      logger.info("Open Shuffle Block: shuffle_" + stageId + "_" + shuffleId);
      long shuffle_array_addr = this.stageArray.getLong(Long.BYTES * stageId);
      if (shuffle_array_addr == 0 && partitionNum < 0) {
        logger.error("shuffle_" + stageId + "_" + shuffleId + " not exists");
        throw new ArrayIndexOutOfBoundsException();  
      } else if (shuffle_array_addr == 0) {
        shuffleArray = this.pmHeap.allocateMemoryBlock(Transactional.class, Long.BYTES * max_shuffles_num);
        this.stageArray.setLong(Long.BYTES * stageId, shuffleArray.address());
      } else {
        shuffleArray = pmHeap.memoryBlockFromAddress(Transactional.class, shuffle_array_addr);
      }

      long block_array_addr = shuffleArray.getLong(Long.BYTES * shuffleId);
      if (block_array_addr == 0 && partitionNum < 0) {
        logger.error("shuffle_" + stageId + "_" + shuffleId + " not exists");
        throw new ArrayIndexOutOfBoundsException();  
      } else if (block_array_addr == 0) {
        shuffleBlock = this.pmHeap.allocateMemoryBlock(Transactional.class, HEADER_SIZE + Long.BYTES * partitionNum);
        shuffleBlock.setInt(0, partitionNum);
        shuffleArray.setLong(Long.BYTES * shuffleId, shuffleBlock.address());
      }
    }

    public MemoryBlock<Transactional> getShuffleBlock(int stageId, int shuffleId) {
      MemoryBlock<Transactional> shuffleArray = null;
      MemoryBlock<Transactional> shuffleBlock = null;
      long shuffle_array_addr = this.stageArray.getLong(Long.BYTES * stageId);
      if (shuffle_array_addr == 0) {
        logger.error("getShuffleBlock: shuffle_" + stageId + " not exists");
        throw new ArrayIndexOutOfBoundsException();  
      } else {
        shuffleArray = pmHeap.memoryBlockFromAddress(Transactional.class, shuffle_array_addr);
      }

      long block_array_addr = shuffleArray.getLong(Long.BYTES * shuffleId);
      if (block_array_addr == 0) {
        logger.error("getShuffleBlock: shuffle_" + stageId + "_" + shuffleId + " not exists");
        throw new ArrayIndexOutOfBoundsException();  
      } else {
        shuffleBlock = pmHeap.memoryBlockFromAddress(Transactional.class, block_array_addr);
      }
      return shuffleBlock;
    }

    public MemoryBlock<Transactional> getPartitionBlock(int stageId, int shuffleId, int partitionId) {
      MemoryBlock<Transactional> shuffleBlock = getShuffleBlock(stageId, shuffleId);
      long partition_block_addr = shuffleBlock.getLong(HEADER_SIZE + Long.BYTES * partitionId);
      if (partition_block_addr == 0) {
        logger.error("getPartitionBlock: shuffle_" + stageId + "_" + shuffleId + "_" + partitionId + " doesn't exist");
        throw new ArrayIndexOutOfBoundsException();
      } 

      return this.pmHeap.memoryBlockFromAddress(Transactional.class, partition_block_addr);
    }

    public int getPartitionNum(int stageId, int shuffleId) {
      return getShuffleBlock(stageId, shuffleId).getInt(0);      
    }

    public void addPartition(int stageId, int shuffleId, int partitionId, long partitionLength) {
      MemoryBlock<Transactional> shuffleBlock = getShuffleBlock(stageId, shuffleId);
      
      int partitionNum = shuffleBlock.getInt(0);
      if (partitionId < 0 || partitionId >= partitionNum) {
        throw new ArrayIndexOutOfBoundsException();
      }

      if (partitionLength > 0) {
        MemoryBlock<Transactional> addr = pmHeap.allocateMemoryBlock(Transactional.class, partitionLength);
        shuffleBlock.setLong(HEADER_SIZE + Long.BYTES * partitionId, addr.address());
      }
    }

    public void set(int stageId, int shuffleId, int partitionId, byte[] value) {
      MemoryBlock<Transactional> partitionBlock = getPartitionBlock(stageId, shuffleId, partitionId);

      partitionBlock.copyFromArray(value, 0, 0, value.length);
      logger.info("write data to shuffleId: " + shuffleId + ", partitionId: " + partitionId + ", length:" + value.length + ", size is " + partitionBlock.size());
    }

    public byte[] get(int stageId, int shuffleId, int partitionId) {
      logger.info("Get Shuffle Block: shuffle_" + stageId + "_" + shuffleId + "_" + partitionId);
      MemoryBlock<Transactional> partitionBlock = getPartitionBlock(stageId, shuffleId, partitionId);

      byte[] bytes = new byte[toIntExact(partitionBlock.size())];
      partitionBlock.copyToArray(0, bytes, 0, toIntExact(partitionBlock.size()));
      return bytes;
    }

    public void close() {
      logger.info("close: free llpl Heap for " + this.path);
      this.pmHeap.freeHeap(this.path); 
    }
}
