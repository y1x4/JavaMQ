package pku;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();

	HashMap<String, DataOutputStream> outMap = new HashMap<>();
    HashMap<String, DataInputStream> inMap  = new HashMap<>();
    HashMap<String, MappedByteBuffer> bufferMap  = new HashMap<>();

    DataOutputStream out;   // 按 topic 写入不同 topic 文件
    DataInputStream in;     // 按 queue + topic 读取 不同 topic 文件
    MappedByteBuffer inBuffer;

	// 加锁保证线程安全
	/**
	 * @param msg
	 * @param topic
	 */
	public synchronized void push(ByteMessage msg, String topic) {
		if (msg == null)
			return;

        try {
            // 获取写入流
            if (!outMap.containsKey(topic)) {
                File file = new File("./data/" + topic);
                if (file.exists()) file.delete();

                outMap.put(topic, new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream("./data/" + topic,  true))));
            }
            out = outMap.get(topic);


            // write headers —— size, key index, valueLength, valueBytes
            out.writeByte(msg.headers().getMap().size());

            String headerKey;
            for (Map.Entry<String, Object> entry : msg.headers().getMap().entrySet()) {
                headerKey = entry.getKey();
                int index = MessageHeader.getHeaderIndex(headerKey);
                if (index == 10) continue;  // 不需要写入TOPIC，读取时根据文件名加入
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

                /*
                out.writeByte(headerKey.getBytes().length); // kLen
                out.write(headerKey.getBytes());

                // headerType: 0 int, 1 long, 2 double, 3 string
                int headerType = MessageHeader.getHeaderType(headerKey);
                if (headerType == 0) {
                    //out.writeByte(4); // 知道int数据类型长度，不需要存
                    out.writeInt((int) entry.getValue());
                } else if (headerType == 1) {
                    //out.writeByte(8);
                    out.writeLong((long) entry.getValue());
                } else if (headerType == 2) {
                    //out.writeByte(8);
                    out.writeDouble((double) entry.getValue());
                } else {
                    String strVal = (String) entry.getValue();
                    out.writeByte(strVal.getBytes().length);
                    out.write(strVal.getBytes());
                }*/

            }

            // write body's length, byte[]
            int bodyLen = msg.getBody().length;
            if (bodyLen <= Byte.MAX_VALUE) {
                out.writeByte(0);
                out.writeByte(bodyLen);
            } else if (bodyLen <= Short.MAX_VALUE){
                out.writeByte(1);  // body[] 的长度 > 127，即超过byte，先存入 1 ，再存入用int表示的长度
                out.writeShort(bodyLen);
            } else {
                out.writeByte(2);
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

            if (! new File("./data/" + topic).exists()) // 不存在此 topic 文件
                return null;

            String key = queue + topic;
            if (!inMap.containsKey(key)) {
                inMap.put(key, new DataInputStream(new BufferedInputStream(new FileInputStream("./data/" + topic))));
            }
            //每个 queue+topic 都有一个InputStream
            in = inMap.get(key);

            // 此 topic 已读取完毕
            if (in.available() == 0) {
                inMap.remove(key);
                return null;
            }

            // 读取 headers 部分
            KeyValue headers = new DefaultKeyValue();
            int headerSize = in.readByte();
            for (int i = 0; i < headerSize; i++) {
                byte kLen = in.readByte();    // keyLength
                byte[] bytes = new byte[kLen];
                in.read(bytes);
                String headerKey = new String(bytes);   // key

                // 0 int, 1 long, 2 double, 3 string
                // System.out.println(headerKey);
                int headerType = MessageHeader.getHeaderIndex(headerKey);

                if (headerType == 0) {
                    headers.put(headerKey, in.readInt());
                } else if (headerType == 1) {
                    headers.put(headerKey, in.readLong());
                } else if (headerType == 2) {
                    headers.put(headerKey, in.readDouble());
                } else {
                    byte vLen = in.readByte();    // valueLength
                    byte[] vals = new byte[vLen];    // value
                    in.read(vals);
                    headers.put(headerKey, new String(vals));
                }
            }

            // 读取 body 部分
            byte isByte = in.readByte();
            byte[] body;
            if (isByte == 0) {
                body = new byte[in.readByte()];
            } else {
                body = new byte[in.readShort()];
            }
            in.read(body);

            // 组成消息并返回
            ByteMessage msg = new DefaultMessage(body);
            msg.setHeaders(headers);
            return msg;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
	}



    // 加锁保证线程安全
    public synchronized ByteMessage pullMBB(String queue, String topic) {
        try {

            if (! new File("./data/" + topic).exists()) // 不存在此 topic 文件
                return null;

            String key = queue + topic;
            if (!bufferMap.containsKey(key)) {

                RandomAccessFile rafi = new RandomAccessFile("./data/" + topic, "r");
                FileChannel fci = rafi.getChannel();
                MappedByteBuffer inBuffer = fci.map(FileChannel.MapMode.READ_ONLY, 0, fci.size());

                bufferMap.put(key, inBuffer);
            }
            inBuffer = bufferMap.get(key);

            // 这个流已经读完
            if (!inBuffer.hasRemaining()) {
                bufferMap.remove(key);
                return null;
            }


            // 读取 headers 部分
            KeyValue headers = new DefaultKeyValue();
            headers.put(MessageHeader.TOPIC, topic);    // 直接写入 topic
            int headerSize = inBuffer.get();
            for (int i = 1; i < headerSize; i++) {  // 少了一轮 topic
                int index = inBuffer.get();

                // 0-3 int, 4-7 long, 8-9 double, 10-15 string
                if (index <= 3) {
                    headers.put(MessageHeader.getHeader(index), inBuffer.getInt());
                } else if (index <= 7) {
                    headers.put(MessageHeader.getHeader(index), inBuffer.getLong());
                } else if (index <= 9) {
                    headers.put(MessageHeader.getHeader(index), inBuffer.getDouble());
                } else {
                    byte vLen = inBuffer.get();    // valueLength
                    byte[] vals = new byte[vLen];    // value
                    inBuffer.get(vals);
                    headers.put(MessageHeader.getHeader(index), new String(vals));
                }


                /*
                byte kLen = inBuffer.get();    // keyLength
                byte[] bytes = new byte[kLen];
                inBuffer.get(bytes);
                String headerKey = new String(bytes);   // key

                // 0 int, 1 long, 2 double, 3 string
                int headerType = MessageHeader.getHeaderType(headerKey);

                if (headerType == 0) {
                    headers.put(headerKey, inBuffer.getInt());
                } else if (headerType == 1) {
                    headers.put(headerKey, inBuffer.getLong());
                } else if (headerType == 2) {
                    headers.put(headerKey, inBuffer.getDouble());
                } else {
                    byte vLen = inBuffer.get();    // valueLength
                    byte[] vals = new byte[vLen];    // value
                    inBuffer.get(vals);
                    headers.put(headerKey, new String(vals));
                }
                */
            }

            // 读取 body 部分
            byte type = inBuffer.get();
            byte[] body;
            if (type == 0) {
                body = new byte[inBuffer.get()];
            } else if (type == 1){
                body = new byte[inBuffer.getShort()];
            } else {
                body = new byte[inBuffer.getInt()];
            }
            inBuffer.get(body);

            // 组成消息并返回
            ByteMessage msg = new DefaultMessage(body);
            msg.setHeaders(headers);
            return msg;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
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
