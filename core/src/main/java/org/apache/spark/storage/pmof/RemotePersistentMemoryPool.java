package org.apache.spark.storage.pmof;

import com.intel.rpmp.PmPoolClient;
import java.nio.ByteBuffer;

public class RemotePersistentMemoryPool {
  private RemotePersistentMemoryPool(String remote_address, String remote_port) {
    pmPoolClient = new PmPoolClient(remote_address, remote_port);
  }

  public static RemotePersistentMemoryPool getInstance(String remote_address, String remote_port) {
    if (instance == null) {
      synchronized (RemotePersistentMemoryPool.class) {
        if (instance == null) {
          instance = new RemotePersistentMemoryPool(remote_address, remote_port);
        }
      }
    }
    return instance;
  }

  public long put(String key, ByteBuffer data, long size) {
    return pmPoolClient.put(key, data, size);
  }

  public long[] getMeta(String key) {
    return pmPoolClient.getMeta(key);
  }

  public int del(String key) {
    return pmPoolClient.del(key);
  }

  private static PmPoolClient pmPoolClient;
  private static RemotePersistentMemoryPool instance;
}