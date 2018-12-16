package pku;

import java.util.*;

/**
 * 消费者
 */

public class Consumer {
    final List<String> topics = new ArrayList<>();
    int readPos = 0;
    String queue;
    int index = 0;

    final PullHelper pullHelper = new PullHelper();

    private BufferService bufferService = BufferService.getInstance("./data/");
    private ArrayList<MessageReader> readers = new ArrayList<>();
    private int pollIndex = 0;
    private int count = 0;

    private MessageSerializer deserializer = new MessageSerializer(); // thread local


    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
    }

    public synchronized void attachQueue2(String queueName, Collection<String> topics) {
        for (String topic: topics) {
            readers.add(new MessageReader(topic, bufferService, deserializer));
        }
    }


    //每次消费读取一个message
    public synchronized ByteMessage poll() {

        // 依次读取 topic 所有内容
        ByteMessage re;

        do {
            re = pullHelper.pull(topics.get(index));
        } while (re == null && ++index < topics.size());

        return re;

    }



     /*
        ByteMessage re = null;
        //先读第一个topic, 再读第二个topic...
        //直到所有topic都读完了, 返回null, 表示无消息
        for (int i = 0; i < topics.size(); i++) {
            int index = (i + readPos) % topics.size();
            re = DemoMessageStore.store.pull(queue, topics.get(index));
            if (re != null) {
                readPos = index + 1;
                break;
            }
        }
        return re;


        MessageReader reader = readers.get(pollIndex);
        ByteMessage message = reader.readMessage();
        while (message == null) {
            readers.remove(pollIndex);
            if (readers.isEmpty()) return null;
            pollIndex = pollIndex % readers.size();
            reader = readers.get(pollIndex);
            message = reader.readMessage();
        }
        if ((++count & 0x3f) == 0) { // change buffer every 64 messages
            pollIndex = (pollIndex + 1) % readers.size();
        }
        return message;
        */





}
