package pku;

import java.io.*;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Future;
import java.util.zip.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
/**
 * 生产者
 */

public class Producer {

    //private final int ONE_WRITE_SIZE = 200;
    private int ONE_WRITE_SIZE;
    byte[] array = new byte[2560000];
    private ByteBuffer buffer = ByteBuffer.wrap(array);
    byte[] temp = new byte[1280000];
    //private ByteBuffer bufferComp = ByteBuffer.allocateDirect(1280000);
    private ByteBuffer mesgBuffer = ByteBuffer.allocateDirect(256256);
    private long currentPos = 0;
    private HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>();
    //private HashMap<String, AsynchronousFileChannel> topicStreams = new HashMap<>();
    private static final Map<String, BufferedOutputStream> topicStreams = new HashMap<>();
    private HashMap<String, Long> writePos = new HashMap<>();
    //AsynchronousFileChannel fileChannel = null;
    BufferedOutputStream fileChannel = null;
    Future<Integer> operation = null;
    boolean flag = false;
    private int msgCount = 0;
    private String className = null;
    private Object hObject = null;
    private String topic = null;
    ArrayList<ByteMessage> messages = null;
    int compLength = 0;
    byte[] bytes = null;
    int bytesLength = 0;
    boolean nameFlag = true;
    int isCompress = 0;
    String name;

    //ArrayList<byte[]> byteHeaders = null;
    //int valueLength = 0;

    private  final String[] headerBank = {
            //Int
            MessageHeader.PRIORITY,
            //Long
            MessageHeader.BORN_TIMESTAMP,
            MessageHeader.STORE_TIMESTAMP,
            MessageHeader.START_TIME,
            MessageHeader.STOP_TIME,
            MessageHeader.TIMEOUT,
            //String
            MessageHeader.MESSAGE_ID,
            MessageHeader.TOPIC,
            MessageHeader.BORN_HOST,
            MessageHeader.STORE_HOST,
            MessageHeader.RELIABILITY,
            MessageHeader.SEARCH_KEY,
            MessageHeader.SCHEDULE_EXPRESSION,
            MessageHeader.SHARDING_KEY,
            MessageHeader.SHARDING_PARTITION,
            MessageHeader.TRACE_ID
    };

    //生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    //将message发送出去
    public void send(ByteMessage defaultMessage){
        topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
        if (!msgs.containsKey(topic)) {
            msgs.put(topic, new ArrayList<>());
        }
        // 加入消息
        if (nameFlag) {
            name = Thread.currentThread().getName();
            if (name.equals("Thread-1") || name.equals("Thread-3")){
                ONE_WRITE_SIZE = 200;
            } else {
                ONE_WRITE_SIZE = 400;
            }
            nameFlag = false;
        }
        msgs.get(topic).add(defaultMessage);
        msgCount++;
        if (msgCount >= ONE_WRITE_SIZE){
            msgCount = 0;
            writeNow();
        }
    }
    //处理将缓存区的剩余部分
    public void flush() {
        writeNow();
//        for (String topic : msgs.keySet()) {
//            //topicStreams.get(topic).flush();
//            //topicStreams.get(topic).close();
//        }
    }


    public void writeNow(){
        for (String topic : msgs.keySet()) {
            messages = msgs.get(topic);
            if (messages.isEmpty()) {
                continue;
            }
            try {
                write(topic, messages);
            } catch (Exception e) {
                e.printStackTrace();
            }
            messages.clear();
        }
    }


    public void write(String topic, List<ByteMessage> messages)throws Exception{

        //String name = Thread.currentThread().getName();
        if (!topicStreams.containsKey(topic)) {
            (new File("data/" + topic)).mkdir();
            currentPos = 0;
            //fileChannel = AsynchronousFileChannel.open(Paths.get("data/" + topic + "/" + name), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            fileChannel = new BufferedOutputStream(new FileOutputStream("data/" + topic + "/" + name, true));
            writePos.put(topic, currentPos);
            topicStreams.put(topic, fileChannel);
        } else {
            fileChannel = topicStreams.get(topic);
        }
        for (ByteMessage b : messages) {

            bytes = message2ByteArray(b);
            bytesLength = bytes.length;
            buffer.put(intToBytes(bytesLength), 0, 4);
            buffer.put(bytes, 0, bytesLength);
        }

//        buffer.flip();
//        compLength = compress(array, buffer.remaining(), temp);
//        fileChannel.write(intToBytes(compLength));
//        fileChannel.write(temp, 0, compLength);
//        fileChannel.flush();
//        buffer.clear();

        buffer.flip();
        if (buffer.remaining() > 50000) {
            isCompress = 1;
            compLength = compress(array, buffer.remaining(), temp);
            fileChannel.write(intToBytes(isCompress));
            fileChannel.write(intToBytes(compLength));
            fileChannel.write(temp, 0, compLength);
        } else {
            isCompress = 0;
            fileChannel.write(intToBytes(isCompress));
            fileChannel.write(intToBytes(buffer.remaining()));
            fileChannel.write(array, 0, buffer.remaining());
        }

        fileChannel.flush();
        buffer.clear();

//        buffer.flip();
//        compLength = compress(array, buffer.remaining(), temp);
//        if (flag) {
//            while (!operation.isDone());
//            bufferComp.clear();
//        }
//        bufferComp.putInt(compLength);
//        bufferComp.put(temp, 0, compLength);
//        bufferComp.flip();
//        currentPos = writePos.get(topic);
//        operation = fileChannel.write(bufferComp, currentPos);
//        writePos.put(topic, currentPos + compLength + 4);
//        buffer.clear();
//        flag = true;
//        if (flag) {
//            while (!operation.isDone());
//            bufferComp.clear();
//        }

//        buffer.flip();
//        int compLength = buffer.remaining();
//        buffer.get(temp, 0 , compLength);
//        if (flag) {
//            while (!operation.isDone());
//            bufferComp.clear();
//        }
//        bufferComp.put(intToBytes(compLength), 0, 4);
//        bufferComp.put(temp, 0, compLength);
//        bufferComp.flip();
//        currentPos = writePos.get(topic);
//        operation = fileChannel.write(bufferComp, currentPos);
//        afterPos = currentPos + compLength + 4;
//        writePos.put(topic, afterPos);
//        buffer.clear();
//        flag = true;
    }

    public byte[] message2ByteArray(ByteMessage message) {
        long flag = 0L;
        //byteHeaders = new ArrayList<>();

        //valueLength = 0;
        mesgBuffer.position(6);

        for (int i = 0; i < 15; i++) {
            if (message.headers().containsKey(headerBank[i])) {
                flag++;
                flag <<= 1;

                hObject = message.headers().getObj(headerBank[i]);
                className = hObject.getClass().toString();
                switch (className){
                    case "class java.lang.Integer":
                        flag <<= 2;
                        int hInt = ((Integer) hObject).intValue();
                        byte[] headerInt = new byte[4];
                        headerInt[0] = (byte) ((hInt >>> 24) & 0xFF);
                        headerInt[1] = (byte) ((hInt >>> 16) & 0xFF);
                        headerInt[2] = (byte) ((hInt >>> 8) & 0xFF);
                        headerInt[3] = (byte) (hInt & 0xFF);
                        //byteHeaders.add(headerInt);
                        mesgBuffer.put(headerInt);
                        //valueLength = valueLength + 4;
                        break;
                    case "class java.lang.Long":
                        flag <<= 1;
                        flag++;
                        flag <<= 1;
                        long hLong = ((Long) hObject).longValue();
                        byte[] headerLong = new byte[8];
                        for (int j = 0; j < 8; j++) {
                            //前在前
                            headerLong[j] = (byte) ((hLong >>> (56 - 8 * j)) & 0xFF);
                        }
                        //byteHeaders.add(headerLong);
                        mesgBuffer.put(headerLong);
                        //valueLength = valueLength + 8;
                        break;
                    case "class java.lang.Double":
                        flag++;
                        flag <<= 2;
                        double hDouble = ((Double) hObject).doubleValue();
                        byte[] headerDouble = new byte[8];
                        long value = Double.doubleToRawLongBits(hDouble);
                        for (int j = 0; j < 8; j++) {
                            //前在后
                            headerDouble[j] = (byte) ((value >>> 8 * j) & 0xff);
                        }
                        //byteHeaders.add(headerDouble);
                        mesgBuffer.put(headerDouble);
                        //valueLength = valueLength + 8;
                        break;
                    case "class java.lang.String":
                        flag++;
                        flag <<= 1;
                        flag++;
                        flag <<= 1;
                        String hString = hObject.toString();
                        char strLength = (char) hString.length();
                        hString = strLength + hString;
                        byte[] headerString = hString.getBytes();
                        //byteHeaders.add(headerString);
                        mesgBuffer.put(headerString);
                        //valueLength = valueLength + headerString.length;
                }

            } else {
                flag <<= 3;
            }
        }

        if (message.headers().containsKey(headerBank[15])) {
            flag++;
            flag <<= 1;
            Object hObject = message.headers().getObj(headerBank[15]);
            className = hObject.getClass().toString();
            switch (className){
                case "class java.lang.Integer":
                    flag <<= 1;
                    int hInt = ((Integer) hObject).intValue();
                    byte[] headerInt = new byte[4];
                    headerInt[0] = (byte) ((hInt >>> 24) & 0xFF);
                    headerInt[1] = (byte) ((hInt >>> 16) & 0xFF);
                    headerInt[2] = (byte) ((hInt >>> 8) & 0xFF);
                    headerInt[3] = (byte) (hInt & 0xFF);
                    //byteHeaders.add(headerInt);
                    mesgBuffer.put(headerInt);
                    //valueLength = valueLength + 4;
                    break;
                case "class java.lang.Long":
                    flag <<= 1;
                    flag++;
                    long hLong = ((Long) hObject).longValue();
                    byte[] headerLong = new byte[8];
                    for (int j = 0; j < 8; j++) {
                        //前在前
                        headerLong[j] = (byte) ((hLong >>> (56 - 8 * j)) & 0xFF);
                    }
                    //byteHeaders.add(headerLong);
                    mesgBuffer.put(headerLong);
                    //valueLength = valueLength + 8;
                    break;
                case "class java.lang.Double":
                    flag++;
                    flag <<= 1;
                    double hDouble = ((Double) hObject).doubleValue();
                    byte[] headerDouble = new byte[8];
                    long value = Double.doubleToRawLongBits(hDouble);
                    for (int j = 0; j < 8; j++) {
                        //前在后
                        headerDouble[j] = (byte) ((value >>> 8 * j) & 0xff);
                    }
                    //byteHeaders.add(headerDouble);
                    mesgBuffer.put(headerDouble);
                    //valueLength = valueLength + 8;
                    break;
                case "class java.lang.String":
                    flag++;
                    flag <<= 1;
                    flag++;
                    String hString = hObject.toString();
                    char strLength = (char) hString.length();
                    hString = strLength + hString;
                    byte[] headerString = hString.getBytes();
                    //byteHeaders.add(headerString);
                    mesgBuffer.put(headerString);
                    //valueLength = valueLength + headerString.length;
            }
        } else {
            flag <<= 2;
        }


        byte[] messBody = message.getBody();

        byte[] flagResult = new byte[8];
        for (int j = 0; j < 8; j++) {
            flagResult[j] = (byte) ((flag >>> (56 - 8 * j)) & 0xFF);
        }

        int i = mesgBuffer.position();
        mesgBuffer.position(0);
        mesgBuffer.put(flagResult, 2, 6);
        mesgBuffer.position(i);
        mesgBuffer.put(messBody);
        mesgBuffer.flip();
        byte[] result = new byte[mesgBuffer.remaining()];
        mesgBuffer.get(result);
        mesgBuffer.clear();


        return result;
    }

    public byte[] intToBytes(int v) {
        byte[] bytes = new byte[4];

        bytes[3] = (byte) (v & 0xff);
        bytes[2] = (byte) ((v >> 8) & 0xff);
        bytes[1] = (byte) ((v >> 16) & 0xff);
        bytes[0] = (byte) ((v >> 24) & 0xff);

        return bytes;
    }



    public int compress(byte[] input, int length, byte[] output) {
        int count = 0;
        int allCount = 0;
        Deflater compressor = new Deflater(1);
        try {
            compressor.setInput(input, 0 , length);
            compressor.finish();
            while (!compressor.finished()) {
                count = compressor.deflate(output, allCount, 1280000);
                allCount = allCount + count;
            }
        } finally {
            compressor.end();
        }
        return allCount;
    }

//    static class writeParallel implements Runnable {
//
//        public writeParallel() {
//
//        }
//
//        @Override
//        public void run() {
//
//        }
//    }
}