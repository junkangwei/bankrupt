package com.bankrupt.client.consumer;

import config.TopicConfig;
import org.bankrupt.remoting.common.RemoteLifeCycle;

import java.util.Set;

public interface Consumer extends RemoteLifeCycle {
    /**
     * 订阅单个
     * @param topic
     * @param messageListener
     */
    void subscription(String topic, MessageListener messageListener);

    /**
     * 订阅多个
     * @param topic
     * @param messageListener
     */
    void subscription(Set<String> topic, MessageListener messageListener);

    /**
     * 订阅topic
     * @param topic
     * @return
     */
    TopicConfig syncTopic(String topic);

}
