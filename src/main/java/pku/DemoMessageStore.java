package pku;

import java.io.*;
import java.nio.ByteBuffer;
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

	static final HashMap<String, DataOutputStream> outMap = new HashMap<>();

    DataOutputStream out;   // 按 topic 写入不同 topic 文件



    static final int BUFFER_CAPACITY = 4660 * 1024;

    HashMap<String, ByteBuffer> bufferMap = new HashMap<>();
    HashMap<String, OutputStream> outputMap = new HashMap<>();
    ByteBuffer buf;
    OutputStream output;



	// 加锁保证线程安全
	public synchronized void push(byte[] header, byte[] body, String topic) {

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


            out.write(header);

            int bodyLen = body.length;
            if (bodyLen <= Byte.MAX_VALUE) {    // body[] 的长度 > 127，即超过byte，先存入 1 ，再存入用int表示的长度
                out.writeByte(0);
                out.writeByte(bodyLen);
            } else {
                out.writeByte(1);
                out.writeInt(bodyLen);
            }

            out.write(body);



        } catch (IOException e) {
            e.printStackTrace();
        }
    }


            /*
            String v14, v13, v12, v11, v10;

            v14 = headers.getString(MessageHeader.TRACE_ID);
            if (v14 != null)
                key = (short) (key | 1);

            key <<= 1;
            v13 = headers.getString(MessageHeader.SCHEDULE_EXPRESSION);
            if (v13 != null)
                key = (short) (key | 1);

            key <<= 1;
            v12 = headers.getString(MessageHeader.SEARCH_KEY);
            if (v12 != null)
                key = (short) (key | 1);

            key <<= 1;
            v11 = headers.getString(MessageHeader.STORE_HOST);
            if (v11 != null)
                key = (short) (key | 1);

            key <<= 1;
            v10 = headers.getString(MessageHeader.BORN_HOST);
            if (v10 != null)
                key = (short) (key | 1);


            double v9, v8;

            key <<= 1;
            v9 = headers.getDouble(MessageHeader.SHARDING_PARTITION);
            if (v9 != 0.0d)
                key = (short) (key | 1);

            key <<= 1;
            v8 = headers.getDouble(MessageHeader.SHARDING_KEY);
            if (v8 != 0.0d)
                key = (short) (key | 1);


            long v7, v6, v5, v4;

            key <<= 1;
            v7 = headers.getLong(MessageHeader.STOP_TIME);
            if (v7 != 0L)
                key = (short) (key | 1);

            key <<= 1;
            v6 = headers.getLong(MessageHeader.START_TIME);
            if (v6 != 0L)
                key = (short) (key | 1);

            key <<= 1;
            v5 = headers.getLong(MessageHeader.STORE_TIMESTAMP);
            if (v5 != 0L)
                key = (short) (key | 1);

            key <<= 1;
            v4 = headers.getLong(MessageHeader.BORN_TIMESTAMP);
            if (v4 != 0L)
                key = (short) (key | 1);


            int v3, v2, v1, v0;

            key <<= 1;
            v3 = headers.getInt(MessageHeader.RELIABILITY);
            if (v3 != 0L)
                key = (short) (key | 1);

            key <<= 1;
            v2 = headers.getInt(MessageHeader.PRIORITY);
            if (v2 != 0L)
                key = (short) (key | 1);

            key <<= 1;
            v1 = headers.getInt(MessageHeader.TIMEOUT);
            if (v1 != 0L)
                key = (short) (key | 1);

            key <<= 1;
            v0 = headers.getInt(MessageHeader.MESSAGE_ID);
            if (v0 != 0L)
                key = (short) (key | 1);



            out.writeShort(key);


            if ((key & 1) == 1) out.writeInt(v0);
            key >>= 1;
            if ((key & 1) == 1) out.writeInt(v1);
            key >>= 1;
            if ((key & 1) == 1) out.writeInt(v2);
            key >>= 1;
            if ((key & 1) == 1) out.writeInt(v3);
            key >>= 1;

            if ((key & 1) == 1) out.writeLong(v4);
            key >>= 1;
            if ((key & 1) == 1) out.writeLong(v5);
            key >>= 1;
            if ((key & 1) == 1) out.writeLong(v6);
            key >>= 1;
            if ((key & 1) == 1) out.writeLong(v7);
            key >>= 1;

            if ((key & 1) == 1) out.writeDouble(v8);
            key >>= 1;
            if ((key & 1) == 1) out.writeDouble(v9);
            key >>= 1;

            if ((key & 1) == 1) {
                strnt[v10.length()]++;
                out.writeByte(v10.length());
                out.write(v10.getBytes());
            }
            key >>= 1;
            if ((key & 1) == 1) {
                strnt[v11.length()]++;
                out.writeByte(v11.length());
                out.write(v11.getBytes());
            }
            key >>= 1;
            if ((key & 1) == 1) {
                strnt[v12.length()]++;
                out.writeByte(v12.length());
                out.write(v12.getBytes());
            }
            key >>= 1;
            if ((key & 1) == 1) {
                strnt[v13.length()]++;
                out.writeByte(v13.length());
                out.write(v13.getBytes());
            }
            key >>= 1;
            if ((key & 1) == 1) {
                strnt[v14.length()]++;
                out.writeByte(v14.length());
                out.write(v14.getBytes());
            }





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


            for (int i = 14; i >= 0; i--) {
                key <<= 1;
                if (headers.containsKey(MessageHeader.getHeader(i)))
                    key = (short) (key | 1);
            }

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






/*


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

*/

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
                //write();
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
            //System.out.println(Arrays.toString(cnt));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // flush
    public void flush() {
        try {
            for (DataOutputStream outputStream : outMap.values()) {
                outputStream.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
