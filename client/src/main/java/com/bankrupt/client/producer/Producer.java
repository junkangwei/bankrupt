package com.bankrupt.client.producer;

import config.TopicConfig;
import org.bankrupt.remoting.common.RemoteLifeCycle;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Producer extends RemoteLifeCycle {
    String send(String topic, Object msg);

    /**
     * 带延迟的
     * @param topic
     * @param msg
     * @param level
     * @return
     */
    String send(String topic, Object msg,Integer level);
    /**
     * 异步发送
     * @param topic
     * @param msg
     * @return
     */
    CompletableFuture<String> asyncSend(String topic, Object msg);

    void createTopic(String topic, int queueNumber);

    TopicConfig syncTopic(String topic);
}
