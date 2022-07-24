package org.bankrupt.broker.topic;

import com.alibaba.fastjson.JSON;
import config.StoreConfig;
import config.TopicConfig;
import org.bankrupt.broker.config.BrokerConfig;
import org.bankrupt.common.ConfigManager;
import org.bankrupt.common.OffsetStore;
import org.bankrupt.common.exception.MQException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认实现类
 */
public class DefaultTopicManage extends ConfigManager {

    private ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);

    public boolean exists(String topic) {
        return topicConfigTable.containsKey(topic);
    }

    public TopicConfig createTopic(String topic, int queueNumber) {
        if (topicConfigTable.containsKey(topic)) {
            throw new MQException("topic : " + topic + " already exists");
        }
        TopicConfig topicConfig = new TopicConfig(topic, queueNumber);
        topicConfigTable.put(topic, topicConfig);
        return topicConfig;
    }

    public TopicConfig getTopic(String topic) {
        TopicConfig topicConfig = null;
        if(topicConfigTable.containsKey(topic)){
            return topicConfigTable.get(topic);
        }else{
            topicConfig = new TopicConfig(topic);
            topicConfigTable.put(topic, topicConfig);
        }
        return topicConfig;
    }

    @Override
    public String configFilePath() {
        return StoreConfig.TOPIC;
    }

    @Override
    public void decode(String jsonString) {
        DefaultTopicManage defaultTopicManage = JSON.parseObject(jsonString, DefaultTopicManage.class);
        if(defaultTopicManage != null){
            this.topicConfigTable = defaultTopicManage.getTopicConfigTable();
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return JSON.toJSONString(this,prettyFormat);
    }

    public ConcurrentHashMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
