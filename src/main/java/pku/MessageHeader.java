package pku;

/**
 * Message的Header的key是一些固定的值, 这里是这些值的列表:
 * 它们的具体意义你不需要关心
 * 你只要保证header的TOPIC字段是指向正确的topic就行了
 */
public class MessageHeader {

    static final String[] headerKeys = {
            "MessageId", "Timeout", "Priority", "Reliability",
            "BornTimestamp", "StoreTimestamp", "StartTime", "StopTime",
            "ShardingKey", "ShardingPartition",
            "BornHost", "StoreHost", "SearchKey", "ScheduleExpression", "TraceId"};



/*
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

*/

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
