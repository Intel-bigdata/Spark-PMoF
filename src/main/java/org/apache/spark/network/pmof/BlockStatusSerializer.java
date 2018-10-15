package org.apache.spark.network.pmof;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class BlockStatusSerializer {
    public BlockStatusSerializer() {
        this.kryo = new Kryo();
        this.kryo.register(BlockStatus.class);
        MapSerializer serializer = new MapSerializer();
        serializer.setKeyClass(String.class, this.kryo.getSerializer(String.class));
        serializer.setKeysCanBeNull(false);
        serializer.setValueClass(BlockStatus.class, null);
        serializer.setValuesCanBeNull(true);
        kryo.register(HashMap.class, serializer);
        kryo.register(BlockStatusMessage.class);
    }

    public ByteBuffer serialize(BlockStatusMessage blockStatusMessage) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
        ByteBufferOutputStream bos = new ByteBufferOutputStream(byteBuffer);
        Output output = new Output(bos);
        kryo.writeObject(output, blockStatusMessage);
        output.flush();
        output.close();
        return byteBuffer;
    }

    public BlockStatusMessage deserialize(ByteBuffer byteBuffer) {
        ByteBufferInputStream bis = new ByteBufferInputStream(byteBuffer);
        byteBuffer.flip();
        Input input;
        BlockStatusMessage blockStatusMessage = null;
        try {
            input = new Input(bis);
            input.close();
            blockStatusMessage = kryo.readObject(input, BlockStatusMessage.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return blockStatusMessage;
    }

    private Kryo kryo;
}
