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
 * partition[0]: 32MB block -> 32MB Block -> ...
 * partition[1]
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

      long shuffle_block_addr = shuffleArray.getLong(Long.BYTES * shuffleId);
      if (shuffle_block_addr == 0 && partitionNum < 0) {
        logger.error("shuffle_" + stageId + "_" + shuffleId + " not exists");
        throw new ArrayIndexOutOfBoundsException();  
      } else if (shuffle_block_addr == 0) {
        shuffleBlock = this.pmHeap.allocateMemoryBlock(Transactional.class, HEADER_SIZE + Long.BYTES * partitionNum * 2);
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

      long shuffle_block_addr = shuffleArray.getLong(Long.BYTES * shuffleId);
      if (shuffle_block_addr == 0) {
        logger.error("getShuffleBlock: shuffle_" + stageId + "_" + shuffleId + " not exists");
        throw new ArrayIndexOutOfBoundsException();  
      } else {
        shuffleBlock = pmHeap.memoryBlockFromAddress(Transactional.class, shuffle_block_addr);
      }
      return shuffleBlock;
    }

    public MemoryBlock<Transactional> getPartitionBlock(int stageId, int shuffleId, int partitionId) {
      MemoryBlock<Transactional> shuffleBlock = getShuffleBlock(stageId, shuffleId);
      long partition_block_addr = shuffleBlock.getLong(HEADER_SIZE + Long.BYTES * partitionId * 2);
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
        // We are using linked block in partition, data structure will be like this
        // next block addr(long) + this block content
        MemoryBlock<Transactional> addr = pmHeap.allocateMemoryBlock(Transactional.class, Long.BYTES + partitionLength);
        
        long partition_block_addr = shuffleBlock.getLong(HEADER_SIZE + Long.BYTES * partitionId * 2);
        long partition_size = shuffleBlock.getLong(HEADER_SIZE + Long.BYTES * (partitionId * 2 + 1));
        MemoryBlock<Transactional> partition_block = null;
        if (partition_block_addr == 0) {
          logger.info("Add first partition block for shuffle_" + stageId + "_" + shuffleId + "_" + partitionId + ", length: " + partitionLength);
          shuffleBlock.setLong(HEADER_SIZE + Long.BYTES * partitionId * 2, addr.address());
        } else {
          do { 
            logger.info("Find last partition block for shuffle_" + stageId + "_" + shuffleId + "_" + partitionId + ", length: " + partitionLength);
            partition_block = this.pmHeap.memoryBlockFromAddress(Transactional.class, partition_block_addr);
            partition_block_addr = partition_block.getLong(0);
          } while (partition_block_addr != 0);
          partition_block.setLong(0, addr.address());
        }
        shuffleBlock.setLong(HEADER_SIZE + Long.BYTES * (partitionId * 2 + 1), partition_size + partitionLength);
      }
    }

    public void set(int stageId, int shuffleId, int partitionId, byte[] value) {
      MemoryBlock<Transactional> partitionBlock = getPartitionBlock(stageId, shuffleId, partitionId);
      // find the last block of this partition
      long partition_block_addr = partitionBlock.getLong(0);
      while (partition_block_addr != 0) {
        partitionBlock = this.pmHeap.memoryBlockFromAddress(Transactional.class, partition_block_addr);
        partition_block_addr = partitionBlock.getLong(0);
      }
      partitionBlock.copyFromArray(value, 0, Long.BYTES, value.length);
      partitionBlock.flush(0, Long.BYTES + value.length);
      logger.info("write data to shuffleId: " + shuffleId + ", partitionId: " + partitionId + ", length:" + value.length + ", size is " + partitionBlock.size());
    }

    public byte[] get(int stageId, int shuffleId, int partitionId) {
      MemoryBlock<Transactional> shuffleBlock = getShuffleBlock(stageId, shuffleId);
      MemoryBlock<Transactional> partitionBlock = null;
      long partition_block_addr = shuffleBlock.getLong(HEADER_SIZE + Long.BYTES * partitionId * 2);
      long partition_length = shuffleBlock.getLong(HEADER_SIZE + Long.BYTES * (partitionId * 2 + 1));

      // since current partition data is using linked block
      // will need to put them into one byte array here
      logger.info("Get Shuffle Block: shuffle_" + stageId + "_" + shuffleId + "_" + partitionId + ", size: " + partition_length);
      byte[] bytes = new byte[toIntExact(partition_length)];
      long cur = 0;
      long tmp_length = 0;
      do {
        partitionBlock = this.pmHeap.memoryBlockFromAddress(Transactional.class, partition_block_addr);
        tmp_length = partitionBlock.size() - Long.BYTES;
        partitionBlock.copyToArray(Long.BYTES, bytes, toIntExact(cur), toIntExact(tmp_length));
        cur += tmp_length;
        partition_block_addr = partitionBlock.getLong(0);
      } while (partition_block_addr != 0);
      return bytes;
    }

    public void close() {
      logger.info("close: free llpl Heap for " + this.path);
      this.pmHeap.freeHeap(this.path); 
    }
}
