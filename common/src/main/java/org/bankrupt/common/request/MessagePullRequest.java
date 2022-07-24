package org.bankrupt.common.request;

public class MessagePullRequest {
    /**
     * 主题
     */
    private String topic;

    /**
     * queueId
     */
    private int queueId;

    /**
     * 消费进度
     */
    private int offset;

    /**
     * 是否持计划
     */
    private Boolean load;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Boolean getLoad() {
        return load;
    }

    public void setLoad(Boolean load) {
        this.load = load;
    }
}
