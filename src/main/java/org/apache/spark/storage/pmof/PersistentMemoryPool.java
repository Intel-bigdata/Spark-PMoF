package org.apache.spark.storage.pmof;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class PersistentMemoryPool {
    static {
        System.load("/usr/local/lib/libjnipmdk.so");
    }
    private static native long nativeOpenDevice(String path, int maxStage, int maxMap, int core_s, int core_e);
    private static native long nativeSetPartition(long deviceHandler, int numPartitions, int stageId, int mapId, int partutionId, long size, byte[] data);
    private static native byte[] nativeGetPartition(long deviceHandler, int stageId, int mapId, int partutionId);
    private static native int nativeCloseDevice(long deviceHandler);
  
    //private static final Logger logger = LoggerFactory.getLogger(PersistentMemoryPool.class);
    //static final int HEADER_SIZE = 8;
    private static final long DEFAULT_PMPOOL_SIZE = 26843545600L;

    private long deviceHandler;

    PersistentMemoryPool(
        String path,
        int max_stages_num,
        int max_shuffles_num,
        long pool_size,
        int core_s,
        int core_e) {

      pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
      this.deviceHandler = nativeOpenDevice(path, max_stages_num, max_shuffles_num, core_s, core_e);
    }

    public long setPartition(int partitionNum, int stageId, int shuffleId, int partitionId, long partitionLength, byte[] data) {
      return nativeSetPartition(this.deviceHandler, partitionNum, stageId, shuffleId, partitionId, partitionLength, data);
    }

    public byte[] getPartition(int stageId, int shuffleId, int partitionId) {
      return nativeGetPartition(this.deviceHandler, stageId, shuffleId, partitionId);
      //nativeGetPartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public void close() {
      nativeCloseDevice(this.deviceHandler);
    }
}
