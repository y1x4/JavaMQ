package pku;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.zip.InflaterOutputStream;

/**
 * 消费者
 */

public class Consumer {
    final List<String> topics = new ArrayList<>();
    int index = 0;
    // int readPos = 0;
    String queue;
    MappedByteBuffer in;

    static int cnt = 0;

    private static final String FILE_DIR = "./data/";


    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
    }


    //每次消费读取一个message
    public ByteMessage poll() {
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


        // 依次读取 topic 所有内容
        ByteMessage re;

        do {
            re = pull(topics.get(index));
        } while (re == null && ++index < topics.size());

        cnt++;
        return re;

    }


    // 加锁保证线程安全
    public ByteMessage pull(String topic) {
        try {


            // if (topic.equals("topic11")) System.out.println(cnt);

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
                        headers.put(MessageHeader.headerKeys[i], in.getInt());
                    else if (i < 8)
                        headers.put(MessageHeader.headerKeys[i], in.getLong());
                    else if (i < 10)
                        headers.put(MessageHeader.headerKeys[i], in.getDouble());
                    else {
                        byte[] vals = new byte[in.get()];    // valueLength
                        in.get(vals);   // value
                        headers.put(MessageHeader.headerKeys[i], new String(vals));
                    }

                }
                key >>= 1;
            }


            // 读取 body 部分
            byte type = in.get();
            byte[] body;
            if (type == 0) {
                body = new byte[in.get()];
                in.get(body);
            } else {
                body = new byte[in.getInt()];
                in.get(body);
                body = decompress(body);
            }


            // 组成消息并返回
            ByteMessage msg = new DefaultMessage(body);
            msg.setHeaders(headers);
            return msg;


        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }



    public static byte[] decompress(byte[] in) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InflaterOutputStream infl = new InflaterOutputStream(out);
            infl.write(in);
            infl.flush();
            infl.close();

            return out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(150);
            return null;
        }
    }
}
