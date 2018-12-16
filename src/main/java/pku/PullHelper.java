package pku;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * Created by yixu on 2018/12/16.
 */
public class PullHelper {

    private static final String FILE_DIR = "./data/";

    MappedByteBuffer in;

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
