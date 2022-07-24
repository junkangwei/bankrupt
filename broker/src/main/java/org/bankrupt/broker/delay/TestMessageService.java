package org.bankrupt.broker.delay;

import com.alibaba.fastjson.JSON;
import config.StoreConfig;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.task.DelayedMessageTask;
import org.bankrupt.common.ConfigManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 创建时间： 2022/7/21 19:44
 *
 * @author 魏俊康
 * 描述： TODO
 */
public class TestMessageService extends ConfigManager{
    private ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

    @Override
    public String configFilePath() {
        return StoreConfig.DELAY;
    }

    @Override
    public void decode(String jsonString) {
        TestMessageService delayMessageService = JSON.parseObject(jsonString, TestMessageService.class);
        if(delayMessageService != null){
            this.offsetTable = delayMessageService.getOffsetTable();
        }
    }

    public static void main(String[] args) {
        final TestMessageService testMessageService = new TestMessageService();
        testMessageService.load();
    }

    @Override
    public String encode(boolean prettyFormat) {
        return JSON.toJSONString(this,prettyFormat);
    }

    public ConcurrentMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }
}
