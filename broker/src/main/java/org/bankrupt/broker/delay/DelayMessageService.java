package org.bankrupt.broker.delay;

import com.alibaba.fastjson.JSON;
import config.StoreConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.config.BrokerConfig;
import org.bankrupt.broker.task.DelayedMessageTask;
import org.bankrupt.broker.topic.DefaultTopicManage;
import org.bankrupt.common.ConfigManager;
import org.bankrupt.common.LifeCycle;
import org.bankrupt.common.OffsetStore;

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
public class DelayMessageService extends ConfigManager implements LifeCycle{

    private static final HashMap<Integer, Long> DELAY_LEVEL_TIME_MAP = new HashMap<>();

    private ConcurrentMap<Integer, Long> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

    private DefaultMessageStore defaultMessageStore;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private ScheduledExecutorService scheduledExecutorService;


    //延时级别 1S 2S 3S 5S 10S 15S 30S 1M 2M 3M 5M 10M 15M 30M 1H 2H
    static {
        DELAY_LEVEL_TIME_MAP.put(1, TimeUnit.SECONDS.toMillis(10));
//        DELAY_LEVEL_TIME_MAP.put(2, TimeUnit.SECONDS.toMillis(2));
//        DELAY_LEVEL_TIME_MAP.put(3, TimeUnit.SECONDS.toMillis(3));
//        DELAY_LEVEL_TIME_MAP.put(4, TimeUnit.SECONDS.toMillis(5));
//        DELAY_LEVEL_TIME_MAP.put(5, TimeUnit.SECONDS.toMillis(10));
//        DELAY_LEVEL_TIME_MAP.put(6, TimeUnit.SECONDS.toMillis(15));
//        DELAY_LEVEL_TIME_MAP.put(7, TimeUnit.SECONDS.toMillis(30));
//        DELAY_LEVEL_TIME_MAP.put(8, TimeUnit.MINUTES.toMillis(1));
//        DELAY_LEVEL_TIME_MAP.put(9, TimeUnit.MINUTES.toMillis(2));
//        DELAY_LEVEL_TIME_MAP.put(10, TimeUnit.MINUTES.toMillis(3));
//        DELAY_LEVEL_TIME_MAP.put(11, TimeUnit.MINUTES.toMillis(5));
//        DELAY_LEVEL_TIME_MAP.put(12, TimeUnit.MINUTES.toMillis(10));
//        DELAY_LEVEL_TIME_MAP.put(13, TimeUnit.MINUTES.toMillis(15));
//        DELAY_LEVEL_TIME_MAP.put(14, TimeUnit.MINUTES.toMillis(30));
//        DELAY_LEVEL_TIME_MAP.put(15, TimeUnit.HOURS.toMillis(1));
//        DELAY_LEVEL_TIME_MAP.put(16, TimeUnit.HOURS.toMillis(2));
    }

    public DelayMessageService() {
    }

    public DelayMessageService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public void start() {
        //启动处理
        if (!started.compareAndSet(false, true)) {
            return;
        }
        this.load();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(17,new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "DelayMessageThread");
            }
        });
        for (Map.Entry<Integer, Long> entry : DELAY_LEVEL_TIME_MAP.entrySet()) {
            int level = entry.getKey();
            Long time = entry.getValue();
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }
            //这边会创建16个任务，延时1S开启
            if (time != null && time > 0) {
                scheduledExecutorService.schedule(new DelayedMessageTask(level,offset,this,defaultMessageStore),1000L,TimeUnit.MILLISECONDS);
            }

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, 3000L, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            this.scheduledExecutorService.shutdown();
        }
    }

    @Override
    public String configFilePath() {
        return StoreConfig.DELAY;
    }

    @Override
    public void decode(String jsonString) {
        DelayMessageService delayMessageService = JSON.parseObject(jsonString, DelayMessageService.class);
        if(delayMessageService != null){
            this.offsetTable = delayMessageService.getOffsetTable();
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return JSON.toJSONString(this,prettyFormat);
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void updateOffset(int delayLevel, long nextOffset) {
        this.offsetTable.put(delayLevel, nextOffset);
    }

    public ConcurrentMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<Integer, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public static HashMap<Integer, Long> getDelayLevelTimeMap() {
        return DELAY_LEVEL_TIME_MAP;
    }
}
