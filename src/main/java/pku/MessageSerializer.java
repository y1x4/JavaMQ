package pku;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 *
 * Created by yixu on 2018/12/11.
 */
public class MessageSerializer {

    private final static byte NUL = (byte)0;

    private final static int START_K = 16;

    private final static byte KEY_HEADER_KEY = (byte)0xff;
    private final static byte KEY_PRO_OFFSET = (byte)0xfe;
    private final static String HEADER_KEY = "MessageId";
    private final static String PRO_OFFSET = "PRO_OFFSET";
    private final static String PRODUCER = "PRODUCER";

    // ATTENTION! Because of this, all reading functions should only be called in single thread
    private final byte[] buf = new byte[101 * 1024];

    public ByteMessage read(ByteBuffer buffer) throws BufferUnderflowException {
        if (!buffer.hasRemaining()) return null;

        byte[] body = readBody(buffer);
        if (body == null) return null;
        ByteMessage message = new DefaultMessage(body);

        KeyValue headers = new DefaultKeyValue();
        int numHeaders = buffer.get(); // one byte
        for (int i = 0; i < numHeaders; i++) {
            byte length = buffer.get(); // one byte
            buffer.get(buf, 0, length);
            String headerKey = new String(buf, 0, length);

            int headerType = MessageHeader.getHeaderIndex(headerKey);

            if (headerType == 0) {
                headers.put(headerKey, buffer.getInt());
            } else if (headerType == 1) {
                headers.put(headerKey, buffer.getLong());
            } else if (headerType == 2) {
                headers.put(headerKey, buffer.getDouble());
            } else {
                byte vLen = buffer.get();    // valueLength
                headers.put(headerKey, new String(buf, 0, vLen));
            }

            message.setHeaders(headers);
        }
        return message;
    }


    private byte[] readBody(ByteBuffer buffer) {
        int position = readCStyleString(buffer);
        if (position == 0) return null;
        return Arrays.copyOf(buf, position);
    }


    private int readCStyleString(ByteBuffer buffer) {
        final int bufferPosition = buffer.position();
        int limit = 0;
        int position = 0;
        for (int k = START_K; position == limit; k <<= 1) {
            try {
                buffer.mark();
                buffer.get(buf, limit, k);
            } catch (BufferUnderflowException ex) {
                buffer.reset();
                k = buffer.remaining();
                buffer.get(buf, limit, k);
            }
            limit += k;
            while (buf[position] != NUL &&  position < limit) {
                position++;
            }
        }
        buffer.position(bufferPosition + position + 1);
        return position;
    }
}
