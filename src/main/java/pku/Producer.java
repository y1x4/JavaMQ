package pku;

import java.util.HashSet;
import java.util.Set;

/**
 * 生产者: 依次遍历 topics 每个 topic 生产 PUSH_COUNT 个消息
 */
public class Producer {

    private Set<String> topics = new HashSet<>();

	// 生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body) {
        topics.add(topic);

        ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC, topic);
        return msg;
    }

    //将message发送出去
    public void send(ByteMessage msg) {
        String topic = msg.headers().getString(MessageHeader.TOPIC);


        // use short to record header keys, except TOPIC
        KeyValue headers = msg.headers();
        short key = 0;

        int len = 2;    // short key
        for (int i = 14; i >= 0; i--) {
            key <<= 1;
            if (headers.containsKey(MessageHeader.headerKeys[i])) {
                key = (short) (key | 1);
                if (i < 4) len += 4;
                else if (i < 10) len += 8;
                else {
                    len += headers.getString(MessageHeader.headerKeys[i]).getBytes().length + 1;
                }
            }
        }

        byte[] header = new byte[len];

        // key
        header[0] = (byte) ((key >>> 8) & 0xFF);
        header[1] = (byte) (key & 0xFF);

        int index = 2;
        int numInt;
        long numLong;
        byte[] strVals;
        for (int i = 0; i < 15; i++) {
            if ((key & 1) == 1) {
                if (i < 4) {
                    numInt = headers.getInt(MessageHeader.headerKeys[i]);
                    header[index++] = (byte) ((numInt >>> 24) & 0xFF);
                    header[index++] = (byte) ((numInt >>> 16) & 0xFF);
                    header[index++] = (byte) ((numInt >>> 8) & 0xFF);
                    header[index++] = (byte) (numInt & 0xFF);
                } else if (i < 8) {
                    numLong = headers.getLong(MessageHeader.headerKeys[i]);
                    header[index++] = (byte) ((numLong >>> 56) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 48) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 40) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 32) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 24) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 16) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 8) & 0xFF);
                    header[index++] = (byte) (numLong & 0xFF);
                } else if (i < 10) {
                    numLong = Double.doubleToLongBits(headers.getDouble(MessageHeader.headerKeys[i]));
                    header[index++] = (byte) ((numLong >>> 56) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 48) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 40) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 32) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 24) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 16) & 0xFF);
                    header[index++] = (byte) ((numLong >>> 8) & 0xFF);
                    header[index++] = (byte) (numLong & 0xFF);
                } else {
                    strVals = headers.getString(MessageHeader.headerKeys[i]).getBytes();
                    header[index++] = (byte) strVals.length;
                    System.arraycopy(strVals, 0, header, index, strVals.length);
                    index += strVals.length;
                }

            }
            key >>= 1;
        }

        DemoMessageStore.store.push(msg.getBody(), topic, header);
    }

    //处理将缓存区的剩余部分
    public void flush()throws Exception {
        DemoMessageStore.store.flush(topics);
        System.out.println("flush");
    }
}
