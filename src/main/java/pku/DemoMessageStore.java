package pku;


import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

public class DemoMessageStore {
    private final static int LENGTH =128*1024*1024;
    private final static int MSG_LENGTH =256*1024;
    private final static int MORE_LENGTH=8*1024*1024;
    public static final DemoMessageStore store = new DemoMessageStore();
    private HashMap<String, MappedByteBuffer> fileout = new HashMap<>();
    private HashMap<String, MappedByteBuffer> filein = new HashMap<>();
    private HashMap<String, Integer> topicLen = new HashMap<>();  //每个topic文件的字节数

    String dir="./data/";
    public synchronized void push(ByteMessage msg, String topic) {
        try {

            MappedByteBuffer javahz = null;
            KeyValue head = msg.headers();       //获取消息头部类
            byte[] body = msg.getBody();         //获取消息body内容
            if (fileout.containsKey(topic)) {      //如果之前写入过topic,则直接获取MappedByteBuffer
                javahz = fileout.get(topic);
            } else {                            //否则创建一个topic文件，并建立内存映射
                File f = new File(dir + File.separator + topic );
                javahz = new RandomAccessFile(f, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, LENGTH);
                topicLen.put(topic, LENGTH);
            }


            //每个消息的数据结构：消息头部键值对个数 key值，value值的类型,[value值的字节数],value值，body的长度，body的内容
            byte size = (byte) head.keySet().size();   //size代表head键值对个数
            javahz.put(size);//存放消息键值对个数
            for (String s : head.keySet()) {   //循环放入消息头部键值对
                javahz.put(keyToByte(s));     //放入key值
                if (String.class.isInstance(head.getObj(s))) {   //如果字符串类型，放入1,
                    javahz.put((byte) 1);
                    String v = head.getString(s);
                    javahz.put((byte)v.length());   //放入value字节数
                    javahz.put(v.getBytes());       //放入value的内容
                } else if (Double.class.isInstance(head.getObj(s))) {  //如果是double类型，放入2，再直接放入value值
                    javahz.put((byte) 2);
                    javahz.putDouble(head.getDouble(s));
                } else if (Integer.class.isInstance(head.getObj(s))) {   //如果是int类型，放入3，再直接放入value值
                    javahz.put((byte) 3);
                    javahz.putInt(head.getInt(s));
                } else if (Long.class.isInstance(head.getObj(s))) { //如果是long类型，放入4，再直接放入value值
                    javahz.put((byte) 4);
                    javahz.putLong(head.getLong(s));
                }
            }
            javahz.putInt(body.length);   //放入body长度（即body的字节数）
            javahz.put(body);    //放入body的内容

            int p = javahz.position();   //记录写入位置，以便扩容
            int len = topicLen.get(topic);  //获取当前映射文件长度
            if (p + MSG_LENGTH > len) {    //判断能否存入下一个消息，不能的话需要进行扩容（初始值100MB每次加100MB）
                File f = new File(dir + File.separator + topic);
                javahz = new RandomAccessFile(f, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len + MORE_LENGTH);
                javahz.position(p);
                topicLen.put(topic, len + MORE_LENGTH);
            }
            fileout.put(topic, javahz);  //保存写入位置


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized ByteMessage pull(String queue, String topic) {
        try {

            String k = queue + " " + topic;
            MappedByteBuffer javahz;  //每个k会对应一个内存映射
            ByteMessage msg = new DefaultMessage(null);  //先生成一个消息
            if (filein.containsKey(k)) {       // private HashMap<String, MappedByteBuffer> filein = new HashMap<>();
                javahz = filein.get(k);
            } else {
                File f = new File(dir + File.separator + topic );
                if (!f.exists()) {       //判断topic文件是否存在，不存在的话返回null，否则建立内存映射
                    return null;
                }
                FileChannel fc = new RandomAccessFile(f, "r").getChannel();
                javahz = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

            }
            byte headNum = javahz.get();  //获取head键值对数量
            if (headNum == 0) {   //如果键值对的个数为0，说明读到文件末尾了，返回null
                return null;
            }
            for (byte i = 0; i < headNum; i++) {  //循环获取head键值对
                String key = byteToKey(javahz.get());
                byte type = javahz.get();  //获取value的类型
                if (type == 1) {  //1代表value的值是String类型
                    byte[] x = new byte[javahz.get()];//获取value值的字节数
                    javahz.get(x);      //获取value的值
                    String st = new String(x);
                    msg.putHeaders(key, st);        //msg放入键值对
                } else if (type == 2) {            //如果是double类型，则直接放入键值对
                    msg.putHeaders(key, javahz.getDouble());
                } else if (type == 3) {              //int类型
                    msg.putHeaders(key, javahz.getInt());
                } else if (type == 4) {                             //Long类型
                    msg.putHeaders(key, javahz.getLong());
                }
            }
            byte[] body = new byte[javahz.getInt()];    //读取消息body的长度
            javahz.get(body);                          //获取消息body的数据
            filein.put(k, javahz);                       //保存读取位置
            msg.setBody(body);                       //赋值msg的body
            return msg;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte keyToByte(String key) throws Exception {
        switch (key) {
            case MessageHeader.TOPIC:
                return (byte) 'a';
            case MessageHeader.SEARCH_KEY:
                return (byte) 'b';
            case MessageHeader.BORN_HOST:
                return (byte) 'c';
            case MessageHeader.BORN_TIMESTAMP:
                return (byte) 'd';
            case MessageHeader.MESSAGE_ID:
                return (byte) 'e';
            case MessageHeader.PRIORITY:
                return (byte) 'f';
            case MessageHeader.RELIABILITY:
                return (byte) 'g';
            case MessageHeader.SCHEDULE_EXPRESSION:
                return (byte) 'h';
            case MessageHeader.SHARDING_KEY:
                return (byte) 'i';
            case MessageHeader.SHARDING_PARTITION:
                return (byte) 'j';
            case MessageHeader.START_TIME:
                return (byte) 'k';
            case MessageHeader.STOP_TIME:
                return (byte) 'l';
            case MessageHeader.STORE_HOST:
                return (byte) 'm';
            case MessageHeader.STORE_TIMESTAMP:
                return (byte) 'n';
            case MessageHeader.TIMEOUT:
                return (byte) 'o';
            case MessageHeader.TRACE_ID:
                return (byte) 'p';
            default:
                throw new Exception("不存在此key");
        }
    }

    public static String byteToKey(byte b) throws Exception {
        switch (b) {
            case 'a':
                return MessageHeader.TOPIC;
            case 'b':
                return MessageHeader.SEARCH_KEY;
            case 'c':
                return MessageHeader.BORN_HOST;
            case 'd':
                return MessageHeader.BORN_TIMESTAMP;
            case 'e':
                return MessageHeader.MESSAGE_ID;
            case 'f':
                return MessageHeader.PRIORITY;
            case 'g':
                return MessageHeader.RELIABILITY;
            case 'h':
                return MessageHeader.SCHEDULE_EXPRESSION;
            case 'i':
                return MessageHeader.SHARDING_KEY;
            case 'j':
                return MessageHeader.SHARDING_PARTITION;
            case 'k':
                return MessageHeader.START_TIME;
            case 'l':
                return MessageHeader.STOP_TIME;
            case 'm':
                return MessageHeader.STORE_HOST;
            case 'n':
                return MessageHeader.STORE_TIMESTAMP;
            case 'o':
                return MessageHeader.TIMEOUT;
            case 'p':
                return MessageHeader.TRACE_ID;
            default:
                throw new Exception("没有byte");
        }

    }
}