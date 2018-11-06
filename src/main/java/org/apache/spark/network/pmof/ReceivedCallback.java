package org.apache.spark.network.pmof;

import java.nio.ByteBuffer;

public interface ReceivedCallback {
    void onSuccess(int blockIndex, int type, ByteBuffer buffer);

    void onFailure(int blockIndex, int type, Throwable e);
}
