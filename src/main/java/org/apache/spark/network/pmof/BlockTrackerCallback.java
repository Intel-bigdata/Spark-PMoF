package org.apache.spark.network.pmof;

import java.nio.ByteBuffer;

public interface BlockTrackerCallback {
    void onSuccess(int chunkIndex, ByteBuffer buffer);
    void onFailure(int chunkIndex, Throwable e);
}
