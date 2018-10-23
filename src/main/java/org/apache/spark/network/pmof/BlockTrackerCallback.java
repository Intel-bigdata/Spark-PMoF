package org.apache.spark.network.pmof;

import java.nio.ByteBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;

public interface BlockTrackerCallback {
    void onSuccess(int chunkIndex, ManagedBuffer buffer);
    void onFailure(int chunkIndex, Throwable e);
}
