package org.apache.spark.network.pmof;

public enum MessageType {
    REGISTER_BLOCK(0), FETCH_BLOCK_STATUS(1), FETCH_BLOCK(2);

    private final byte id;

    MessageType(int id) {
        assert id < 128 : "Cannot have more than 128 message types";
        this.id = (byte) id;
    }

    public byte id() { return id; }
}
