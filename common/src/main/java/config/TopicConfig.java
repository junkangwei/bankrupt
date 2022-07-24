package config;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * topic配置类
 */
public class TopicConfig {
    //topic的名称
    private String topicName;

    //队列数量
    private int queueNumber = 4;

    private AtomicInteger index = new AtomicInteger();


    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int queueNumber) {
        this.topicName = topicName;
        this.queueNumber = queueNumber;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public void setQueueNumber(int queueNumber) {
        this.queueNumber = queueNumber;
    }

    public Integer selectQueueId(){
        return index.getAndAdd(1) % queueNumber;
    }
}
