package pku;

import java.nio.ByteBuffer;

/**
 *
 * Created by yixu on 2018/12/11.
 */
public class MessageReader {

    private final String bucket;
    private final ByteBuffer buffer;
    private final MessageSerializer deserializer;

    public MessageReader(String bucket, BufferService bufferService, MessageSerializer deserializer) {
        this.bucket = bucket;
        this.buffer = bufferService.getBuffer(bucket);
        this.deserializer = deserializer;
    }

    public ByteMessage readMessage() {
        ByteMessage message = deserializer.read(buffer);
        if (message == null)
            return null;
        message.putHeaders(MessageHeader.TOPIC, bucket);
        return message;
    }

}
