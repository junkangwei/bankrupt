package org.bankrupt.common.response;

import message.Message;

import java.nio.ByteBuffer;
import java.util.List;

public class MessagePullResponse {

    private List<Message> messageList;

    private String topic;

    private Integer queueId;

    //下次拉取的offset
    private Long nextOffset;

    //还有多少count
    private int count;

    public List<Message> getMessageList() {
        return messageList;
    }

    public void setMessageList(List<Message> messageList) {
        this.messageList = messageList;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(Long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
