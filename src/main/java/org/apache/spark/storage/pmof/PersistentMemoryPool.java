package org.apache.spark.storage.pmof;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class PersistentMemoryPool {
    static {
        System.load("/usr/local/lib/libjnipmdk.so");
    }
    private static native long nativeOpenDevice(String path, int maxStage, int maxMap, long size);
    private static native long nativeSetMapPartition(long deviceHandler, int numPartitions, int stageId, int mapId, int partutionId, long size, byte[] data, boolean clean);
    private static native long nativeSetReducePartition(long deviceHandler, int numPartitions, int stageId, int partutionId, long size, byte[] data, boolean clean);
    private static native byte[] nativeGetMapPartition(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native byte[] nativeGetReducePartition(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long nativeGetRoot(long deviceHandler);
    private static native int nativeCloseDevice(long deviceHandler);
  
    //private static final Logger logger = LoggerFactory.getLogger(PersistentMemoryPool.class);
    //static final int HEADER_SIZE = 8;
    private static final long DEFAULT_PMPOOL_SIZE = 0L;

    private String device;
    private long deviceHandler;

    PersistentMemoryPool(
        String path,
        int max_stages_num,
        int max_shuffles_num,
        long pool_size) {
      this.device = path; 
      pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
      this.deviceHandler = nativeOpenDevice(path, max_stages_num, max_shuffles_num, pool_size);
    }

    public long setMapPartition(int partitionNum, int stageId, int shuffleId, int partitionId, long partitionLength, byte[] data, boolean clean) {
      return nativeSetMapPartition(this.deviceHandler, partitionNum, stageId, shuffleId, partitionId, partitionLength, data, clean);
    }

    public long setReducePartition(int partitionNum, int stageId, int partitionId, long partitionLength, byte[] data, boolean clean) {
        return nativeSetReducePartition(this.deviceHandler, partitionNum, stageId, partitionId, partitionLength, data, clean);
    }

    public byte[] getMapPartition(int stageId, int shuffleId, int partitionId) {
      return nativeGetMapPartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public byte[] getReducePartition(int stageId, int shuffleId, int partitionId) {
      return nativeGetReducePartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long getRootAddr() {
        return nativeGetRoot(this.deviceHandler);
    }

    public void close() {
      nativeCloseDevice(this.deviceHandler);
      try {
        System.out.println("Use pmempool to delete device:" + device);
        Runtime.getRuntime().exec("pmempool rm " + device);
      } catch (Exception e) {
        System.err.println("delete " + device + "failed: " + e.getMessage());
      }
    }
}
