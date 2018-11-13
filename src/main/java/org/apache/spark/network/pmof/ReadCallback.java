package org.apache.spark.network.pmof;

public interface ReadCallback {
    void onSuccess(int blockIndex, ShuffleBuffer buffer);

    void onFailure(int blockIndex, Throwable e);
}
