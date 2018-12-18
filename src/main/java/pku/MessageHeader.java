package pku;

/**
 * Message的Header的key是一些固定的值, 这里是这些值的列表:
 * 它们的具体意义你不需要关心
 * 你只要保证header的TOPIC字段是指向正确的topic就行了
 */
public class MessageHeader {

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
