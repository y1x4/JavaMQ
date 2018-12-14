package pku;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

/**
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();

    private static final String FILE_DIR = "./data/";

	HashMap<String, DataOutputStream> outMap = new HashMap<>();
    HashMap<String, MappedByteBuffer> inMap  = new HashMap<>();


    DataOutputStream out;   // 按 topic 写入不同 topic 文件
    MappedByteBuffer in;     // 按 queue + topic 读取 不同 topic 文件



    static final int BUFFER_CAPACITY = 4660 * 1024;

    HashMap<String, ByteBuffer> bufferMap = new HashMap<>();
    HashMap<String, OutputStream> outputMap = new HashMap<>();
    ByteBuffer buf;
    OutputStream output;



	// 加锁保证线程安全
	public synchronized void push(ByteMessage msg, String topic) {
		if (msg == null)
			return;

        try {

            // 获取写入流
            if (!outMap.containsKey(topic)) {
                File file = new File("./data/" + topic);
                if (file.exists()) file.delete();

                outMap.put(topic, new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(FILE_DIR + topic, true))));
            }
            out = outMap.get(topic);


            // use short to record header keys, except TOPIC
            KeyValue headers = msg.headers();
            short key = 0;
            for (int i = 0; i < 15; i++) {
                key = (short) (key << 1);
                if (headers.containsKey(MessageHeader.getHeader(14 - i)))
                    key = (short) (key | 1);
            }
            out.writeShort(key);
            for (int i = 0; i < 4; i++) {
                if ((key >> i & 1) == 1)
                    out.writeInt(headers.getInt(MessageHeader.getHeader(i)));
            }
            for (int i = 4; i < 8; i++) {
                if ((key >> i & 1) == 1)
                    out.writeLong(headers.getLong(MessageHeader.getHeader(i)));
            }
            for (int i = 8; i < 10; i++) {
                if ((key >> i & 1) == 1)
                    out.writeDouble(headers.getDouble(MessageHeader.getHeader(i)));
            }
            for (int i = 11; i < 15; i++) {
                if ((key >> i & 1) == 1) {
                    String strVal = headers.getString(MessageHeader.getHeader(i));
                    out.writeByte(strVal.getBytes().length);
                    out.write(strVal.getBytes());
                }
            }
/*

            // write headers —— size, key index, valueLength, valueBytes
            out.writeByte(msg.headers().getMap().size());
            String headerKey;
            for (Map.Entry<String, Object> entry : msg.headers().getMap().entrySet()) {
                headerKey = entry.getKey();
                int index = MessageHeader.getHeaderIndex(headerKey);
                if (index == 15) continue;  // 不需要写入TOPIC，读取时根据文件名加入
                out.writeByte(index);

                // 0-3, 4-7, 8-9, 10-15
                if (index <= 3) {
                    //out.writeByte(4); // 知道int数据类型长度，不需要存
                    out.writeInt((int) entry.getValue());
                } else if (index <= 7) {
                    //out.writeByte(8);
                    out.writeLong((long) entry.getValue());
                } else if (index <= 9) {
                    //out.writeByte(8);
                    out.writeDouble((double) entry.getValue());
                } else {
                    String strVal = (String) entry.getValue();
                    out.writeByte(strVal.getBytes().length);
                    out.write(strVal.getBytes());
                }
            }



*/


            // write body's length, byte[]
            int bodyLen = msg.getBody().length;
            if (bodyLen <= Byte.MAX_VALUE) {// body[] 的长度 > 127，即超过byte，先存入 1 ，再存入用int表示的长度
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


    // 加锁保证线程安全
    public synchronized ByteMessage pull(String queue, String topic) {
        try {

            String inKey = queue + topic;
            in = inMap.get(inKey);
            if (in == null) {   // 不含则新建 buffer
                File file = new File(FILE_DIR + topic );
                if (!file.exists()) {       //判断topic文件是否存在，不存在的话返回null，否则建立内存映射
                    return null;
                }

                FileChannel fc = new RandomAccessFile(file, "r").getChannel();
                in = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

                inMap.put(inKey, in);
            }

            // 这个流已经读完
            if (!in.hasRemaining()) {
                inMap.remove(inKey);
                return null;
            }



            // 读取 headers 部分
            KeyValue headers = new DefaultKeyValue();
            headers.put(MessageHeader.TOPIC, topic);    // 直接写入 topic
            short key = in.getShort();
            for (int i = 0; i < 4; i++) {
                if ((key >> i & 1) == 1)
                    headers.put(MessageHeader.getHeader(i), in.getInt());
            }
            for (int i = 4; i < 8; i++) {
                if ((key >> i & 1) == 1)
                    headers.put(MessageHeader.getHeader(i), in.getLong());
            }
            for (int i = 8; i < 10; i++) {
                if ((key >> i & 1) == 1)
                    headers.put(MessageHeader.getHeader(i), in.getDouble());
            }
            for (int i = 11; i < 15; i++) {
                if ((key >> i & 1) == 1) {
                    byte[] vals = new byte[in.get()];    // valueLength
                    in.get(vals);   // value
                    headers.put(MessageHeader.getHeader(i), new String(vals));
                }
            }


            /*

            // 读取 headers 部分
            KeyValue headers = new DefaultKeyValue();
            headers.put(MessageHeader.TOPIC, topic);    // 直接写入 topic
            int headerSize = in.get();
            for (int i = 1; i < headerSize; i++) {  // 少了一轮 topic
                int index = in.get();

                // 0-3 int, 4-7 long, 8-9 double, 10-15 string
                if (index <= 3) {
                    headers.put(MessageHeader.getHeader(index), in.getInt());
                } else if (index <= 7) {
                    headers.put(MessageHeader.getHeader(index), in.getLong());
                } else if (index <= 9) {
                    headers.put(MessageHeader.getHeader(index), in.getDouble());
                } else {
                    byte vLen = in.get();    // valueLength
                    byte[] vals = new byte[vLen];    // value
                    in.get(vals);
                    headers.put(MessageHeader.getHeader(index), new String(vals));
                }
            }


 */

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





    // 加锁保证线程安全
    public synchronized void bufferPush(ByteMessage msg, String topic) {
        if (msg == null)
            return;

        try {
            // 获取写入流
            out = outMap.get(topic);
            if (out == null) {
                File file = new File("./data/" + topic);
                // if (file.exists()) file.delete();

                buf = ByteBuffer.allocateDirect(BUFFER_CAPACITY);
                output = new BufferedOutputStream(new FileOutputStream(file, true), BUFFER_CAPACITY);
                bufferMap.put(topic, buf);
                outputMap.put(topic, output);
            }


            // use short to record header keys, except TOPIC
            KeyValue headers = msg.headers();
            short key = 0;
            for (int i = 0; i < 15; i++) {
                key = (short) (key << 1);
                if (headers.containsKey(MessageHeader.getHeader(14 - i)))
                    key = (short) (key | 1);
            }
            buf.putShort(key);
            for (int i = 0; i < 4; i++) {
                if ((key >> i & 1) == 1)
                    buf.putInt(headers.getInt(MessageHeader.getHeader(i)));
            }
            for (int i = 4; i < 8; i++) {
                if ((key >> i & 1) == 1)
                    buf.putLong(headers.getLong(MessageHeader.getHeader(i)));
            }
            for (int i = 8; i < 10; i++) {
                if ((key >> i & 1) == 1)
                    buf.putDouble(headers.getDouble(MessageHeader.getHeader(i)));
            }
            for (int i = 11; i < 15; i++) {
                if ((key >> i & 1) == 1) {
                    String strVal = headers.getString(MessageHeader.getHeader(i));
                    buf.put((byte) strVal.getBytes().length);
                    buf.put(strVal.getBytes());
                }
            }


            // write body's length, byte[]
            int bodyLen = msg.getBody().length;
            if (bodyLen <= Byte.MAX_VALUE) {
                buf.put((byte) 0);
                buf.put((byte) bodyLen);
            } else if (bodyLen <= Short.MAX_VALUE){
                buf.put((byte) 1);  // body[] 的长度 > 127，即超过byte，先存入 1 ，再存入用int表示的长度
                buf.putShort((short) bodyLen);
            } else {
                buf.put((byte) 2);
                buf.putInt(bodyLen);
            }
            buf.put(msg.getBody());


            // 如果 buffer 剩余空间不足，先写入此 buffer
            if (buf.remaining() <= 201 * 1024) {

                System.out.println("----------------");
                write();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void write() throws Exception {
        if (buf.remaining() == BUFFER_CAPACITY) {
            return;
        }

        buf.putShort((short) -1);//17 means to be continue
        byte[] bytes = new byte[buf.position()];
        buf.position(0);
        buf.get(bytes);
        // bytes = compress(bytes);
        System.out.println("----------------");
        writeInt(bytes.length);
        output.write(bytes, 0, bytes.length);
        //output.flush();
        buf.clear();
    }

    public void writeInt(int a) throws Exception{
        byte[] b = new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)};
        output.write(b, 0, 4);
    }



    public static byte[] compress(byte[] in) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DeflaterOutputStream defl = new DeflaterOutputStream(out);
            defl.write(in);
            defl.flush();
            defl.close();

            return out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(150);
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



    // buffer flush
    public void bFlush(Set<String> topics) {
        try {
            for (String topic : topics) {
                buf    = bufferMap.get(topic);
                output = outputMap.get(topic);
                write();
                // writeInt(-1); 没有了返回 -1
                output.flush();
                //output.close();
            }
        } catch (Exception e) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
