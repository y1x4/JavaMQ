package pku;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * 消费者
 */

public class Consumer {
    final List<String> topics = new ArrayList<>();
    int readPos = 0;
    String queue;
    int index = 0;

    //final PullHelper pullHelper = new PullHelper();
    private static final String FILE_DIR = "./data/";

    MappedByteBuffer in;

    private BufferService bufferService = BufferService.getInstance("./data/");
    private ArrayList<MessageReader> readers = new ArrayList<>();
    private int pollIndex = 0;
    private int count = 0;

    private MessageSerializer deserializer = new MessageSerializer(); // thread local


    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
    }

    public synchronized void attachQueue2(String queueName, Collection<String> topics) {
        for (String topic: topics) {
            readers.add(new MessageReader(topic, bufferService, deserializer));
        }
    }


    //每次消费读取一个message
    public ByteMessage poll() {

        // 依次读取 topic 所有内容
        ByteMessage re;

        do {
            re = pull(topics.get(index));
        } while (re == null && ++index < topics.size());

        return re;

    }



        /*
        ByteMessage re = null;
        //先读第一个topic, 再读第二个topic...
        //直到所有topic都读完了, 返回null, 表示无消息
        for (int i = 0; i < topics.size(); i++) {
            int index = (i + readPos) % topics.size();
            re = DemoMessageStore.store.pull(queue, topics.get(index));
            if (re != null) {
                readPos = index + 1;
                break;
            }
        }
        return re;
        */


    // 加锁保证线程安全
    public ByteMessage pull(String topic) {
        try {

            // String inKey = queue + topic;
            // in = inMap.get(inKey);
            if (in == null) {   // 不含则新建 buffer
                File file = new File(FILE_DIR + topic );
                if (!file.exists()) {       //判断topic文件是否存在，不存在的话返回null，否则建立内存映射
                    return null;
                }

                FileChannel fc = new RandomAccessFile(file, "r").getChannel();
                in = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

                // inMap.put(inKey, in);
            }

            // 这个流已经读完
            if (!in.hasRemaining()) {
                in = null;
                // inMap.remove(inKey);
                return null;
            }


            // 读取 headers 部分
            KeyValue headers = new DefaultKeyValue();
            headers.put(MessageHeader.TOPIC, topic);    // 直接写入 topic

            short key = in.getShort();

            for (int i = 0; i < 15; i++) {
                if ((key & 1) == 1) {
                    if (i < 4)
                        headers.put(MessageHeader.getHeader(i), in.getInt());
                    else if (i < 8)
                        headers.put(MessageHeader.getHeader(i), in.getLong());
                    else if (i < 10)
                        headers.put(MessageHeader.getHeader(i), in.getDouble());
                    else {
                        byte[] vals = new byte[in.get()];    // valueLength
                        in.get(vals);   // value
                        headers.put(MessageHeader.getHeader(i), new String(vals));
                    }

                }
                key >>= 1;
            }


            // 读取 body 部分
            byte type = in.get();
            byte[] body;
            if (type == 0) {
                body = new byte[in.get()];
            } else {
                body = new byte[in.getInt()];
            }
            in.get(body);


            // 组成消息并返回
            ByteMessage msg = new DefaultMessage(body);
            msg.setHeaders(headers);
            return msg;


        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
