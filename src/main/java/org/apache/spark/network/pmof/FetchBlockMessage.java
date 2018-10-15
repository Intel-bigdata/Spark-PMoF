package org.apache.spark.network.pmof;

import java.io.Serializable;

public class FetchBlockMessage implements Serializable {
    public MessageType type() {
        return MessageType.FETCH_BLOCK_STATUS;
    }
}
