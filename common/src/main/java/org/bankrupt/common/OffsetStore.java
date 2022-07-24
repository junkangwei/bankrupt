package org.bankrupt.common;

import com.alibaba.fastjson.JSON;
import config.StoreConfig;

import java.util.concurrent.ConcurrentHashMap;

public class OffsetStore extends ConfigManager{

    private static final String DELIMITER = "@";

    /**
     * key是: topic@queueId
     * value是偏移量
     */
    private ConcurrentHashMap<String,Integer> offsetTable = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, Integer> getOffsetTable() {
        return offsetTable;
    }

    public Integer getNextOffset(String topic, Integer queueId) {
        String key = topic + DELIMITER + queueId;
        return offsetTable.get(key);
    }

    /**
     * 更新消费进度
     * @param topic
     * @param queueId
     * @param commitOffset
     */
    public void updateOffset(String topic, Integer queueId, int commitOffset) {
        String key = topic + DELIMITER + queueId;
        offsetTable.put(key,commitOffset);
    }

    @Override
    public String configFilePath() {
        return StoreConfig.CONSUME;
    }

    @Override
    public void decode(String jsonString) {
        OffsetStore offsetStore = JSON.parseObject(jsonString, OffsetStore.class);
        if(offsetStore != null){
            this.offsetTable = offsetStore.getOffsetTable();
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return JSON.toJSONString(this,prettyFormat);
    }
}
