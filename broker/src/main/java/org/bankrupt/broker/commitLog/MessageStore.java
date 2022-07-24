package org.bankrupt.broker.commitLog;

import org.bankrupt.broker.store.MessageGetResult;

public interface MessageStore {

    /**
     * 加载消息
     * @return
     */
    boolean load();

    void start();

    void shutDown();

    /**
     * 获得消息
     * @param offset 偏移量
     * @param queueId queueid
     * @param topic topic地址
     * @param load
     * @return
     */
    MessageGetResult getMessage(int offset, int queueId, String topic, Boolean load);
}
