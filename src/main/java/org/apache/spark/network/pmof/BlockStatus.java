package org.apache.spark.network.pmof;

public class BlockStatus {
    public long getPysicalAddr() {
        return pysicalAddr;
    }

    public void setPysicalAddr(long pysicalAddr) {
        this.pysicalAddr = pysicalAddr;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "BlockStatus: [pysicalAddr=" + this.pysicalAddr + ", key=" + this.key + "]";
    }

    private long pysicalAddr;
    private long key;
}
