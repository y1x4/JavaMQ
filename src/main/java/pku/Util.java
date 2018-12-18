package pku;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
public class Util {
    static final String OBJ_INT = "class java.lang.Integer";
    static final String OBJ_LONG = "class java.lang.Long";
    static final String OBJ_DOUBLE = "class java.lang.Double";
    static final String OBJ_STRING = "class java.lang.String";

    public static final String DATA_PATH = "data/";
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

    public static byte[] toBytes(int v) {
        byte[] bytes = new byte[4];

        bytes[3] = (byte) (v & 0xff);
        bytes[2] = (byte) ((v >> 8) & 0xff);
        bytes[1] = (byte) ((v >> 16) & 0xff);
        bytes[0] = (byte) ((v >> 24) & 0xff);

        return bytes;
    }

    public static byte[] message2ByteArray(ByteMessage message) {
        long flag = 0L;
        List<byte[]> byteHeaders = new ArrayList<>();

        //value
        for (int i = 0; i < 15; i++) {
            if (message.headers().containsKey(headerBank[i])) {
                flag++;
                flag <<= 1;
                Object hObject = message.headers().getObj(headerBank[i]);

                //Int
                if (hObject.getClass().toString().equals(OBJ_INT)) {
                    flag <<= 2;

                    int hInt = ((Integer) hObject).intValue();
                    byte[] headerInt = new byte[4];
                    headerInt[0] = (byte) ((hInt >>> 24) & 0xFF);
                    headerInt[1] = (byte) ((hInt >>> 16) & 0xFF);
                    headerInt[2] = (byte) ((hInt >>> 8) & 0xFF);
                    headerInt[3] = (byte) (hInt & 0xFF);
                    byteHeaders.add(headerInt);
                } else if (hObject.getClass().toString().equals(OBJ_LONG)) {
                    flag <<= 1;
                    flag++;
                    flag <<= 1;

                    long hLong = ((Long) hObject).longValue();
                    byte[] headerLong = new byte[8];
                    for (int j = 0; j < 8; j++) {
                        //前在前
                        headerLong[j] = (byte) ((hLong >>> (56 - 8 * j)) & 0xFF);
                    }
                    byteHeaders.add(headerLong);
                } else if (hObject.getClass().toString().equals(OBJ_DOUBLE)) {
                    flag++;
                    flag <<= 2;

                    double hDouble = ((Double) hObject).doubleValue();
                    byte[] headerDouble = new byte[8];
                    long value = Double.doubleToRawLongBits(hDouble);
                    for (int j = 0; j < 8; j++) {
                        //前在后
                        headerDouble[j] = (byte) ((value >>> 8 * j) & 0xff);
                    }
                    byteHeaders.add(headerDouble);
                } else if (hObject.getClass().toString().equals(OBJ_STRING)) {
                    flag++;
                    flag <<= 1;
                    flag++;
                    flag <<= 1;

                    String hString = hObject.toString();
                    char strLength = (char) hString.length();
                    hString = strLength + hString;
                    byte[] headerString = hString.getBytes();
                    byteHeaders.add(headerString);
                }
            } else {
                flag <<= 3;
            }
        }

        if (message.headers().containsKey(headerBank[15])) {
            flag++;
            flag <<= 1;
            Object hObject = message.headers().getObj(headerBank[15]);

            //Int
            if (hObject.getClass().toString().equals(OBJ_INT)) {
                flag <<= 1;

                int hInt = ((Integer) hObject).intValue();
                byte[] headerInt = new byte[4];
                headerInt[0] = (byte) ((hInt >>> 24) & 0xFF);
                headerInt[1] = (byte) ((hInt >>> 16) & 0xFF);
                headerInt[2] = (byte) ((hInt >>> 8) & 0xFF);
                headerInt[3] = (byte) (hInt & 0xFF);
                byteHeaders.add(headerInt);
            } else if (hObject.getClass().toString().equals(OBJ_LONG)) {
                flag <<= 1;
                flag++;

                long hLong = ((Long) hObject).longValue();
                byte[] headerLong = new byte[8];
                for (int j = 0; j < 8; j++) {
                    headerLong[j] = (byte) ((hLong >>> (64 - 8 * j)) & 0xFF);
                }
                byteHeaders.add(headerLong);
            } else if (hObject.getClass().toString().equals(OBJ_DOUBLE)) {
                flag++;
                flag <<= 1;

                double hDouble = ((Double) hObject).doubleValue();
                byte[] headerDouble = new byte[8];
                long value = Double.doubleToRawLongBits(hDouble);
                for (int j = 0; j < 8; j++) {
                    headerDouble[j] = (byte) ((value >>> 8 * j) & 0xff);
                }
                byteHeaders.add(headerDouble);
            } else if (hObject.getClass().toString().equals(OBJ_STRING)) {
                flag++;
                flag <<= 1;
                flag++;

                String hString = hObject.toString();
                char strLength = (char) hString.length();
                hString = strLength + hString;
                byte[] headerString = hString.getBytes();
                byteHeaders.add(headerString);
            }
        } else {
            flag <<= 2;
        }

        //判断结果数组长度
        int valueLength = 0;
        for (byte[] tmp : byteHeaders) {
            valueLength += tmp.length;
        }
        byte[] messBody = message.getBody();
        byte[] result = new byte[6 + valueLength + messBody.length];
        byte[] flagResult = new byte[8];
        for (int j = 0; j < 8; j++) {
            //前在前
            flagResult[j] = (byte) ((flag >>> (56 - 8 * j)) & 0xFF);
        }
        System.arraycopy(flagResult, 2, result, 0, 6);

        int pos = 6;
        for (byte[] tmp : byteHeaders) {
            System.arraycopy(tmp, 0, result, pos, tmp.length);
            pos += tmp.length;
        }

        System.arraycopy(messBody, 0, result, pos, messBody.length);

        return result;
    }

    public static int toInt(byte[] bytes) {
        return (bytes[3] & 0xff) |
                ((bytes[2] & 0xff) << 8) |
                ((bytes[1] & 0xff) << 16) |
                ((bytes[0] & 0xff) << 24);
    }

    public static DefaultMessage byteArray2Message(byte[] getByte) {
        DefaultMessage message = new DefaultMessage(null);
        KeyValue headers = new DefaultKeyValue();
        long note_header =(Long.valueOf(getByte[0] & 0xff) << 40) + (Long.valueOf(getByte[1] & 0xff) << 32) + (Long.valueOf(getByte[2] & 0xff) << 24) + (Long.valueOf(getByte[3] & 0xff) << 16) + (Long.valueOf(getByte[4] & 0xff) << 8) + (Long.valueOf(getByte[5] & 0xff) << 0);
        int label = 0; //标记key
        int index = 6; //标记value
        int check = 47; //标记note
        for (int num = 0; num < 16; num++) { //处理16个header的key
            if ((((note_header) >> check) & 1) == 1) {
                if (((((note_header) >> check-1) & 1) == 0) && ((((note_header) >> check-2) & 1) == 0)) {//说明是int类型
                    int mid = 0;
                    int value_int = 0;
                    for (int j = 0; j < 4; j++) {
                        mid = getByte[index + j];
                        mid = (mid & 0xff)<< 8*(3-j);
                        value_int = value_int + mid;
                    }

                    headers.put(headerBank[label],value_int);
                    index = index + 4;
                }else if (((((note_header) >> check-1) & 1) == 0) && ((((note_header) >> check-2) & 1) == 1)) { //说明是long类型
                    ByteBuffer buffer = ByteBuffer.allocate(8);
                    buffer.put(getByte, index, 8);
                    buffer.flip();//need flip
                    headers.put(headerBank[label],buffer.getLong());
                    index = index + 8;
                }else if (((((note_header) >> check-1) & 1) == 1) && ((((note_header) >> check-2) & 1) == 0)) { //说明是double类型
                    byte[] mid = new byte[8];
                    System.arraycopy(getByte, index, mid,0, mid.length);
                    long value = 0;
                    for (int i = 0; i < 8; i++) {
                        value |= ((long) (mid[i] & 0xff)) << (8 * i);
                    }
                    Double value_double = Double.longBitsToDouble(value);
                    headers.put(headerBank[label],value_double);
                    index = index + 8;
                }else if (((((note_header) >> check-1) & 1) == 1) && ((((note_header) >> check-2) & 1) == 1)) { //说明是str类型
                    String value_str = "";
                    int value_len = getByte[index++] & 0xff;
                    for (int j = 0; j < value_len; j++) {
                        value_str = value_str + String.valueOf((char)getByte[index++]);
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
}
