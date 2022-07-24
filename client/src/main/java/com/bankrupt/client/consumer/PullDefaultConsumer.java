package com.bankrupt.client.consumer;

import com.bankrupt.client.MQClientAPIImpl;
import com.bankrupt.client.config.ClientConfig;
import com.bankrupt.client.producer.DefaultProducer;
import config.TopicConfig;
import org.apache.log4j.Logger;
import org.bankrupt.common.OffsetStore;
import org.bankrupt.common.exception.MQException;


import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 拉取消息的类
 */
public class PullDefaultConsumer implements Consumer{

    public static Logger log = Logger.getLogger(PullDefaultConsumer.class);

    private MQClientAPIImpl mqClientAPI;

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final BlockingQueue<Runnable> consumeRequestQueue = new LinkedBlockingDeque<>();

    private ClientConfig config;

    private MessageListener messageListener;

    private PullMessageService pullMessageService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "ConsumerMQScheduledThread");
        }
    });

    private final Map<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);

    public PullDefaultConsumer(ClientConfig config) {
        this.config = config;
        this.pullMessageService = new PullMessageService(this);
    }

    public void registerListener(MessageListener listener){
        this.messageListener = listener;
        pullMessageService.setMessageListener(listener);
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                mqClientAPI = new MQClientAPIImpl(config);
                mqClientAPI.start();
                initOffset();
                startExecutor();
                pullMessageService.start();
            } catch (Exception e) {
                throw new IllegalStateException("start error", e);
            }
        }
    }

    private void startExecutor() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    updateTopicOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicOffset exception", e);
                }
            }
        }, 10, 30_000, TimeUnit.MILLISECONDS);
    }

    private void updateTopicOffset() {

    }

    /**
     * 去consume取获queue的偏移量
     */
    private void initOffset() {
        Set<Map.Entry<String, TopicConfig>> entries = topicConfigTable.entrySet();
        for (Map.Entry<String, TopicConfig> entry : entries) {

        }
    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            mqClientAPI.close();
            pullMessageService.close();
            scheduledExecutorService.shutdown();
        }
    }

    @Override
    public void subscription(String topic, MessageListener messageListener) {
        if(!this.start.get()){
            try {
//                TopicConfig topicConfig = syncTopic(topic);
                TopicConfig topicConfig = topicConfigTable.get(topic);
                if(topicConfig == null){
                    topicConfigTable.put(topic,new TopicConfig(topic));
                }
                this.messageListener = messageListener;
                this.pullMessageService.setMessageListener(messageListener);
                this.pullMessageService.setTopicConfigTable(topicConfigTable);
            }catch (Exception e){
                throw new MQException("订阅失败的topic是:" + e.getMessage());
            }
        }else{
            throw new MQException("订阅失败");
        }
    }

    @Override
    public void subscription(Set<String> topics, MessageListener messageListener) {
        for (String topic : topics) {
            try {
                subscription(topic,messageListener);
            }catch (Exception e){
//                log.error("同步失败的topic是:{},异常原因是:{}",topic,e);
                log.error("同步失败:" + topic);
            }
        }
    }

    @Override
    public TopicConfig syncTopic(String topic) {
        return mqClientAPI.syncTopic(topic);
    }

    public MQClientAPIImpl getMqClientAPI() {
        return mqClientAPI;
    }

    public ClientConfig getConfig() {
        return config;
    }
}
