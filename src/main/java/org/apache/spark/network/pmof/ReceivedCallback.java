package org.apache.spark.network.pmof;

import java.nio.ByteBuffer;

public interface ReceivedCallback {
    void onSuccess(int blockIndex, ByteBuffer buffer);

    void onFailure(int blockIndex, Throwable e);
}
