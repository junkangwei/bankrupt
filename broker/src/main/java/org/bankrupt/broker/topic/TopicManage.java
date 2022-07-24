package org.bankrupt.broker.topic;

import config.TopicConfig;
import org.bankrupt.common.LifeCycle;

public interface TopicManage extends LifeCycle{
    /**
     * broker启动初始化topic
     */
    void initTopic();

    /**
     * 是否含有topic
     *
     * @param topic
     * @return
     */
    boolean exists(String topic);

    /**
     * 创建topic
     * @param topic
     * @param queueNumber
     */
    TopicConfig createTopic(String topic, int queueNumber);

    /**
     * 创建topic
     * @param topic
     */
    TopicConfig getTopic(String topic);
}
