package org.apache.spark.network.pmof;

import java.io.Serializable;
import java.util.HashMap;

public class BlockStatusMessage implements Serializable {
    public BlockStatusMessage() {
        this.blockStatusMap = new HashMap<String, BlockStatus>();
    }

    public MessageType type() {
        return MessageType.REGISTER_BLOCK;
    }

    public HashMap<String, BlockStatus> getBlockStatusMap() {
        return blockStatusMap;
    }

    public void enqueue(String blockId, BlockStatus blockStatus) {
        this.blockStatusMap.put(blockId, blockStatus);
    }

    public void enqueue(BlockStatusMessage blockStatusMessage) {
        blockStatusMessage.getBlockStatusMap().forEach((k, v)->this.blockStatusMap.put(k, v));
    }

    private HashMap<String, BlockStatus> blockStatusMap;
}
