package pku;

import java.util.HashMap;
import java.util.Map;

/**
 * Message的Header的key是一些固定的值, 这里是这些值的列表:
 * 它们的具体意义你不需要关心
 * 你只要保证header的TOPIC字段是指向正确的topic就行了
 */
public class MessageHeader {

    private static final String[] headerKeys = {
            "MessageId", "Timeout", "Priority", "Reliability",
            "BornTimestamp", "StoreTimestamp", "StartTime", "StopTime",
            "ShardingKey", "ShardingPartition",
            "BornHost", "StoreHost", "SearchKey", "ScheduleExpression", "TraceId"};

    // 使用 short 的16位表示 header key

    // 0-3 int, 4-7 long, 8-9 double, 10-15 string
    private static final Map<String, Integer> typeMap = new HashMap<String, Integer>(){{
        put(MESSAGE_ID, 0);
        put(TIMEOUT, 1);
        put(PRIORITY, 2);
        put(RELIABILITY, 3);

        put(BORN_TIMESTAMP, 4);
        put(STORE_TIMESTAMP, 5);
        put(START_TIME, 6);
        put(STOP_TIME, 7);

        put(SHARDING_KEY, 8);
        put(SHARDING_PARTITION, 9);

        put(BORN_HOST, 10);
        put(STORE_HOST, 11);
        put(SEARCH_KEY, 12);
        put(SCHEDULE_EXPRESSION, 13);
        put(TRACE_ID, 14);
        put(TOPIC, 15);
    }};

    private static final Map<Integer, String> headMap = new HashMap<Integer, String>(){{
        put(0, MESSAGE_ID);
        put(1, TIMEOUT);
        put(2, PRIORITY);
        put(3, RELIABILITY);

        put(4, BORN_TIMESTAMP);
        put(5, STORE_TIMESTAMP);
        put(6, START_TIME);
        put(7, STOP_TIME);

        put(8, SHARDING_KEY);
        put(9, SHARDING_PARTITION);

        put(10, BORN_HOST);
        put(11, STORE_HOST);
        put(12, SEARCH_KEY);
        put(13, SCHEDULE_EXPRESSION);
        put(14, TRACE_ID);
        put(15, TOPIC);
    }};

    // get headers' type
    public static int getHeaderIndex(String header) {
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
            out.writeByte(v10.length());
            out.write(v10.getBytes());
        }
        key >>= 1;
        if ((key & 1) == 1) {
            out.writeByte(v11.length());
            out.write(v11.getBytes());
        }
        key >>= 1;
        if ((key & 1) == 1) {
            out.writeByte(v12.length());
            out.write(v12.getBytes());
        }
        key >>= 1;
        if ((key & 1) == 1) {
            out.writeByte(v13.length());
            out.write(v13.getBytes());
        }
        key >>= 1;
        if ((key & 1) == 1) {
            out.writeByte(v14.length());
            out.write(v14.getBytes());
        }*/
        return typeMap.get(header);
    }


    public static String getHeader(int index) {
        return headerKeys[index];
    }

    //对于每个producer的唯一标识
    //类型: int
    public static final String MESSAGE_ID = "MessageId";

    //topic名字
    //类型: string
    public static final String TOPIC = "Topic";

    //创建时的时间戳
    //类型: long
    public static final String BORN_TIMESTAMP = "BornTimestamp";

    //生产者的hostname
    //类型: string
    public static final String BORN_HOST = "BornHost";

    //服务器存储时间
    //类型: long
    public static final String STORE_TIMESTAMP = "StoreTimestamp";

    //服务器的hostname
    //类型: string
    public static final String STORE_HOST = "StoreHost";

    //消息开始时间
    //类型: long
    public static final String START_TIME = "StartTime";

    //消息结束时间
    //类型: long
    public static final String STOP_TIME = "StopTime";

    //超时时间
    //类型: int
    public static final String TIMEOUT = "Timeout";

    //优先级
    //类型: int
    public static final String PRIORITY = "Priority";

    //可靠性等级
    //类型: int
    public static final String RELIABILITY = "Reliability";

    //搜索key
    //类型: string
    public static final String SEARCH_KEY = "SearchKey";

    //SCHEDULE_EXPRESSION
    //类型: string
    public static final String SCHEDULE_EXPRESSION = "ScheduleExpression";

    //SHARDING_KEY
    //类型: double
    public static final String SHARDING_KEY = "ShardingKey";

    //SHARDING_PARTITION
    //类型: double
    public static final String SHARDING_PARTITION = "ShardingPartition";

    //TRACE_ID
    //类型: string
    public static final String TRACE_ID = "TraceId";

}
