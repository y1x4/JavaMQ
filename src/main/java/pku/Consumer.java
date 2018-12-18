package pku;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.*;
/**
 * 消费者
 */

public class Consumer {
    static final String[] headerBank = {
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

    private final int ONE_READ_SIZE = 600;
    private ArrayList<String> topics = new ArrayList<>();
    private ArrayList<ByteMessage> messageList = new ArrayList<>();
    private int readMesgListPos = 0;
    byte[] array = new byte[2560000];
    private ByteBuffer bufferUncomp = ByteBuffer.wrap(array);
    byte[] singleMessage = null;
    private HashMap<String, ArrayList<BufferedInputStream>> topicInput = new HashMap<>();
    int readPos = 0;
    String queue;
    ByteMessage re = null;
    int className = 0;
    int currentReadMesg = 0;
    File file = null;
    File[] arrayFile = null;
    int uncompLength;
    int messageLength;
    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) {
//        if (queue != null) {
//            throw new Exception("只允许绑定一次");
//        }
        queue = queueName;
        topics.addAll(t);
    }


    //每次消费读取一个message
    public ByteMessage poll() throws Exception {


        String topicName = null;
        while (true){
            topicName = topics.get(readPos);
            re = loadByteMessage(topicName);
            if (re == null){
                readPos++;
                if (readPos >= topics.size()){
                    return null;
                }
            } else {
                break;
            }
        }

        return re;
    }

    public ByteMessage loadByteMessage(String topicName) throws IOException {


        //ByteMessage result;
        while (true) {
            if (messageList.isEmpty()) {
                if (!topicInput.containsKey(topicName)) {
                    try {
                        file = new File("data/" + topicName);
                        if (!file.exists()) {
                            return null;
                        }
                        arrayFile = file.listFiles();
                        topicInput.put(topicName, new ArrayList<>());
                        for (File x : arrayFile) {
                            topicInput.get(topicName).add(new BufferedInputStream(new FileInputStream(x)));
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                for (BufferedInputStream x : topicInput.get(topicName)) {
                    try {
                        messageList.addAll(readMessage(x));
                    } catch (NullPointerException e) {
                        continue;
                    }

                    readMesgListPos = 0;
                    break;
                }
                if (messageList.isEmpty()) {
                    return null;
                }
            }

            if (readMesgListPos < currentReadMesg) {
                readMesgListPos++;
                return messageList.get(readMesgListPos - 1);
            } else {
                messageList.clear();
            }
        }

    }

    public ArrayList<ByteMessage> readMessage(BufferedInputStream buffer) throws IOException {
        ArrayList<ByteMessage> result = new ArrayList<>();
        //uncompLength = 0;
        //int messageLength = 0;

//        选择压缩版本
        byte[] length = new byte[4];
        if (buffer.read(length) == -1) {
            return null;
        }

        int isCompress = toInt(length);
        buffer.read(length);
        int allMessageLength = toInt(length);
        byte[] msg = new byte[allMessageLength];
        buffer.read(msg);
        if (isCompress == 1) {
            try {
            uncompLength = uncompress(msg, array);
            bufferUncomp.limit(uncompLength);
            } catch (DataFormatException e){
                e.printStackTrace();
            }
            int i;
            for (i = 0; i < ONE_READ_SIZE; i++){
                if (bufferUncomp.remaining() == 0){
                    currentReadMesg = i;
                    bufferUncomp.clear();
                    return result;
                }
                messageLength = bufferUncomp.getInt();
                singleMessage = new byte[messageLength];
                bufferUncomp.get(singleMessage);
                result.add(byteArray2Message(singleMessage));
            }
            currentReadMesg = i;
        } else {
            bufferUncomp.put(msg, 0, msg.length);
            bufferUncomp.flip();
            int i = 0;
            for (i = 0; i < ONE_READ_SIZE; i++){

                if (bufferUncomp.remaining() == 0){
                    currentReadMesg = i;
                    bufferUncomp.clear();
                    return result;
                }
                messageLength = bufferUncomp.getInt();
                singleMessage = new byte[messageLength];
                bufferUncomp.get(singleMessage);
                result.add(byteArray2Message(singleMessage));
            }
            currentReadMesg = i;
        }


//        有压缩版本
//        byte[] length = new byte[4];
//        if (buffer.read(length) == -1) {
//            return null;
//        }
//
//        int allMessageLength = toInt(length);
//
//        byte[] msg = new byte[allMessageLength];
//        buffer.read(msg);
//
//        try {
//            uncompLength = uncompress(msg, array);
//            bufferUncomp.limit(uncompLength);
//        } catch (DataFormatException e){
//            e.printStackTrace();
//        }
//        int i;
//        for (i = 0; i < ONE_READ_SIZE; i++){
//            if (bufferUncomp.remaining() == 0){
//                currentReadMesg = i;
//                bufferUncomp.clear();
//                return result;
//            }
//            messageLength = bufferUncomp.getInt();
//            singleMessage = new byte[messageLength];
//            bufferUncomp.get(singleMessage);
//            result.add(byteArray2Message(singleMessage));
//        }

//        无压缩版本的
//        bufferUncomp.put(msg, 0, msg.length);
//        bufferUncomp.flip();
//        int i = 0;
//        for (i = 0; i < ONE_READ_SIZE; i++){
//
//            if (bufferUncomp.remaining() == 0){
//                currentReadMesg = i;
//                bufferUncomp.clear();
//                return result;
//            }
//            messageLength = bufferUncomp.getInt();
//            singleMessage = new byte[messageLength];
//            bufferUncomp.get(singleMessage);
//            result.add(byteArray2Message(singleMessage));
//        }

        //currentReadMesg = i;
        bufferUncomp.clear();
        return result;
    }

    public int toInt(byte[] bytes) {
        return (bytes[3] & 0xff) |
                ((bytes[2] & 0xff) << 8) |
                ((bytes[1] & 0xff) << 16) |
                ((bytes[0] & 0xff) << 24);
    }

    public DefaultMessage byteArray2Message(byte[] getByte) {
        DefaultMessage message = new DefaultMessage(null);
        KeyValue headers = new DefaultKeyValue();
        long note_header =(Long.valueOf(getByte[0] & 0xff) << 40) + (Long.valueOf(getByte[1] & 0xff) << 32) + ((getByte[2] & 0xff) << 24) + ((getByte[3] & 0xff) << 16) + ((getByte[4] & 0xff) << 8) + ((getByte[5] & 0xff) << 0);
        int label = 0; //标记key
        int index = 6; //标记value
        int check = 47; //标记note
        for (int num = 0; num < 16; num++) { //处理16个header的key

            if ((((note_header) >> check) & 1) == 1) {
                className = (int)(((((note_header) >> check-1) & 1) << 1) + (((note_header) >> check-2) & 1));
                switch (className){
                    case 0:
                        int mid = 0;
                        int value_int = 0;
                        for (int j = 0; j < 4; j++) {
                            mid = getByte[index + j];
                            mid = (mid & 0xff)<< 8*(3-j);
                            value_int = value_int + mid;
                        }
                        headers.put(headerBank[label],value_int);
                        index = index + 4;
                        break;
                    case 1:
                        ByteBuffer buffer = ByteBuffer.allocate(8);
                        buffer.put(getByte, index, 8);
                        buffer.flip();//need flip
                        headers.put(headerBank[label],buffer.getLong());
                        index = index + 8;
                        break;
                    case 2:
                        byte[] miid = new byte[8];
                        System.arraycopy(getByte, index, miid,0, miid.length);
                        long value = 0;
                        for (int i = 0; i < 8; i++) {
                            value |= ((long) (miid[i] & 0xff)) << (8 * i);
                        }
                        Double value_double = Double.longBitsToDouble(value);
                        headers.put(headerBank[label],value_double);
                        index = index + 8;
                        break;
                    case 3:
                        String value_str = "";
                        int value_len = getByte[index++] & 0xff;
                        for (int j = 0; j < value_len; j++) {
                            value_str = value_str + ((char)getByte[index++]);
                        }
                        headers.put(headerBank[label],value_str);
                }

            }
            label++;
            check = check - 3;
        }
        //开始处理body
        byte[] body = new byte[getByte.length-index];
        System.arraycopy(getByte, index, body,0, body.length);
        message.setBody(body);
        message.setHeaders(headers);

        return message;
    }


    public int uncompress(byte[] input, byte[] output) throws DataFormatException {
        int count = 0;
        int allCount = 0;
        Inflater decompressor = new Inflater();
        try {
            decompressor.setInput(input);
            while (!decompressor.finished()) {
                count = decompressor.inflate(output, allCount, 2560000);
                allCount = allCount + count;
            }
        } finally {
            decompressor.end();
        }
        return allCount;
    }

}
