package message;

import java.util.Map;
import java.util.concurrent.locks.Lock;

public class CommitLogMessage {
    byte[] body;
    private Long bornTime;
    private String topic;
    private Integer queueId;
    private Long queueOffset;
    private String header;


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
}
