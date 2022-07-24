package org.bankrupt.common.request;

import java.util.HashMap;
import java.util.Map;

/**
 * 创建topic
 */
public class CreateTopicRequest {
    /**
     * topic名称
     */
    private String topic;
    /**
     * 队列id
     */
    private int queueNumber;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public void setQueueNumber(int queueNumber) {
        this.queueNumber = queueNumber;
    }
}
