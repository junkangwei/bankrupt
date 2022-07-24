package org.bankrupt.broker.topic;

import java.util.Map;

/**
 * topic配置类
 */
public class TopicConfig {
    //topic的名称
    private String topicName;

    //队列数量
    private int queueNumber = 4;


    public TopicConfig(String topicName) {
        this.topicName = topicName;
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
}
