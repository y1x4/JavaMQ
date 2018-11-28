package pku;

/**
 * 生产者
 */
public class Producer {

	//生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    //将message发送出去
    public void send(ByteMessage defaultMessage){
        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
        DemoMessageStore.store.push(defaultMessage,topic);
    }
    //处理将缓存区的剩余部分
    public void flush()throws Exception{
        System.out.println(1);
    }
}
