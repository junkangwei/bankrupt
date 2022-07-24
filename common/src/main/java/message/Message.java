package message;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.locks.Lock;


public class Message {
    byte[] body;
    private Long bornTime;
    private String topic;
    private Integer queueId;
    private Long queueOffset;
    private String header;
    private int consumeTime;

    // 消息所处的offset
    private int offset;
    //下一个消息的offset
    private int nextOffset;

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(int nextOffset) {
        this.nextOffset = nextOffset;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Long getBornTime() {
        return bornTime;
    }

    public void setBornTime(Long bornTime) {
        this.bornTime = bornTime;
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

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public int getConsumeTime() {
        return consumeTime;
    }

    public void setConsumeTime(int consumeTime) {
        this.consumeTime = consumeTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "body=" + Arrays.toString(body) +
                ", bornTime=" + bornTime +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", queueOffset=" + queueOffset +
                ", header='" + header + '\'' +
                ", consumeTime=" + consumeTime +
                ", offset=" + offset +
                ", nextOffset=" + nextOffset +
                '}';
    }
}
