package org.bankrupt.broker.store;

import message.Message;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MessageGetResult {
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private int totalSize = 0;
    private Boolean found;
    private final List<Message> messageList = new ArrayList<Message>(100);

    public Boolean getFound() {
        return found;
    }

    public void setFound(Boolean found) {
        this.found = found;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }

    public List<Message> getMessageList() {
        return messageList;
    }

    public void addMessage(Message message) {
        this.messageList.add(message);
    }
}
