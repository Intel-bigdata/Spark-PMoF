package org.apache.spark.storage.pmof;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import java.io.IOException;

public class PersistentMemoryPool {
    static {
        System.loadLibrary("jnipmdk");
    }
    private static native long nativeOpenDevice(String path, int maxStage, int maxMap);
    private static native int nativeSetPartition(long deviceHandler, int numPartitions, int stageId, int mapId, int partutionId, long size, byte[] data);
    private static native byte[] nativeGetPartition(long deviceHandler, int stageId, int mapId, int partutionId);
    private static native int nativeCloseDevice(long deviceHandler);
  
    //private static final Logger logger = LoggerFactory.getLogger(PersistentMemoryPool.class);
    static final int HEADER_SIZE = 8;
    static final long DEFAULT_PMPOOL_SIZE = 26843545600L;
    int max_stages_num;
    int max_shuffles_num;

    private long deviceHandler;
    
    PersistentMemoryPool(
        String path,
        int max_stages_num,
        int max_shuffles_num,
        long pool_size) {
      this.max_stages_num = max_stages_num;
      this.max_shuffles_num = max_shuffles_num;

      //logger.info("Open pmdk_handler [" + path + "]");
      pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
      this.deviceHandler = nativeOpenDevice(path, max_stages_num, max_shuffles_num);
    }

    public void setPartition(int partitionNum, int stageId, int shuffleId, int partitionId, long partitionLength, byte[] data) {
      nativeSetPartition(this.deviceHandler, partitionNum, stageId, shuffleId, partitionId, partitionLength, data);
    }

    public byte[] getPartition(int stageId, int shuffleId, int partitionId) {
      return nativeGetPartition(this.deviceHandler, stageId, shuffleId, partitionId);
      //nativeGetPartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public void close() {
      nativeCloseDevice(this.deviceHandler);
    }
}
