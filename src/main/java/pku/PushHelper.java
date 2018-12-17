package pku;

import java.io.*;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * Created by yixu on 2018/12/16.
 */
public class PushHelper {
    static final PushHelper pushHelper = new PushHelper();

    private static final String FILE_DIR = "./data/";

    static final HashMap<String, DataOutputStream> outMap = new HashMap<>();

    DataOutputStream out;   // 按 topic 写入不同 topic 文件


    // 加锁保证线程安全
    public synchronized void push(ByteMessage msg, String topic) {
        if (msg == null)
            return;

        try {

            // 获取写入流
            out = outMap.get(topic);
            if (out == null) {
                File file = new File(FILE_DIR + topic);
                if (file.exists()) file.delete();

                out = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(file, true), 32768));
                outMap.put(topic, out);
            }


            // use short to record header keys, except TOPIC
            KeyValue headers = msg.headers();
            short key = 0;
            int len = 0;


            for (int i = 14; i >= 0; i--) {
                key <<= 1;
                if (headers.containsKey(MessageHeader.headerKeys[i]))
                    key = (short) (key | 1);
            }
            out.writeShort(key);

            for (int i = 0; i < 15; i++) {
                if ((key & 1) == 1) {
                    if (i < 4)
                        out.writeInt(headers.getInt(MessageHeader.headerKeys[i]));
                    else if (i < 8)
                        out.writeLong(headers.getLong(MessageHeader.headerKeys[i]));
                    else if (i < 10)
                        out.writeDouble(headers.getDouble(MessageHeader.headerKeys[i]));
                    else {
                        String strVal = headers.getString(MessageHeader.headerKeys[i]);
                        out.writeByte(strVal.getBytes().length);
                        out.write(strVal.getBytes());
                    }

                }
                key >>= 1;
            }

            // write body's length, byte[]
            int bodyLen = msg.getBody().length;
            if (bodyLen <= Byte.MAX_VALUE) {    // body[] 的长度 > 127，即超过byte，先存入 1 ，再存入用int表示的长度
                out.writeByte(0);
                out.writeByte(bodyLen);
            } else {
                out.writeByte(1);
                out.writeInt(bodyLen);
            }
            out.write(msg.getBody());




        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    // flush
    public void flush(Set<String> topics) {
        DataOutputStream out;
        try {
            for (String topic : topics) {
                out = outMap.get(topic);
                out.flush();
            }
            //System.out.println(Arrays.toString(cnt));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
