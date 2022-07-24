package com.bankrupt.client.producer;

import com.bankrupt.client.MQClientAPIImpl;
import com.bankrupt.client.config.ClientConfig;
import config.TopicConfig;
import org.apache.log4j.Logger;
import org.bankrupt.common.constant.HeaderConstant;
import org.bankrupt.common.exception.MQException;
import org.bankrupt.common.request.CreateTopicRequest;
import org.bankrupt.common.request.MessageAddRequest;
import org.bankrupt.remoting.common.RemotingCommand;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * 发消息和创建topic，同步topic的信息
 */
public class DefaultProducer implements Producer{

    private static Logger log = Logger.getLogger(DefaultProducer.class);

    private final ClientConfig config;

    private MQClientAPIImpl mqClientAPI;

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final BlockingQueue<Runnable> consumeRequestQueue = new LinkedBlockingDeque<>();

    private ExecutorService sendExecutorService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "ProducerMQScheduledThread");
        }
    });

    private final Map<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);

    public DefaultProducer(ClientConfig config) {
        this.config = config;
        sendExecutorService =  new ThreadPoolExecutor(
                15, //30
                30, //30
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue);
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                mqClientAPI = new MQClientAPIImpl(config);
                mqClientAPI.start();
                sendExecutorService = Executors.newFixedThreadPool(this.config.getSendThreadNum());
                startExecutor();
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
                    syncTopic();
                } catch (Exception e) {
                    log.error("同步topic", e);
                }
            }
        }, 10, 30_000, TimeUnit.MILLISECONDS);
    }

    /**
     * 同步topic
     */
    private void syncTopic() {
        Set<String> strings = topicConfigTable.keySet();
        for (String topic : strings) {
            final TopicConfig topicConfig = syncTopic(topic);
            if(topicConfig == null){
                topicConfigTable.remove(topic);
            }else{
                topicConfigTable.put(topic,topicConfig);
            }
        }
    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            mqClientAPI.close();
            scheduledExecutorService.shutdown();
        }
    }

    public String send(String topic, Map<String, String> headerMap, Object msg) {
        MessageAddRequest req = new MessageAddRequest();
        //寻找header
        try {
            TopicConfig topicConfig = topicConfigTable.get(topic);
            if(topicConfig == null){
                TopicConfig tc = syncTopic(topic);
                topicConfigTable.put(topic,tc);
                topicConfig = tc;
            }
            Integer queueId = topicConfig.selectQueueId();
            System.out.println("topic:" + topic + " queueId:" + queueId);
            headerMap.put(HeaderConstant.QUEUE_ID,queueId.toString());
            headerMap.put(HeaderConstant.TOPIC,topic);
            req.setTopic(topic);
            req.setHeader(headerMap);
            req.setBody(RemotingCommand.encode(msg));
            return mqClientAPI.send(req,headerMap);
        }catch (Exception e){
            throw new MQException(e.getMessage());
        }
    }

    @Override
    public String send(String topic, Object msg) {
        return send(topic,msg,null);
    }

    @Override
    public String send(String topic, Object msg, Integer level) {
        Map<String, String> headerMap = new HashMap<>();
        if(level != null){
            headerMap.put(HeaderConstant.LEVEL,level.toString());
        }
        return send(topic, headerMap,msg);
    }

    public CompletableFuture<String> asyncSend(String topic, Map<String, String> header, Object msg) {
        return asyncSend(() -> {
            return send(topic, header, msg);
        });
    }

    @Override
    public CompletableFuture<String> asyncSend(String topic, Object msg) {
        return asyncSend(topic,Collections.emptyMap(),msg);
    }

    @Override
    public void createTopic(String topic, int queueNumber) {
        if(topicConfigTable.containsKey(topic)){
            throw new MQException("当前topic已经创建了");
        }
        CreateTopicRequest createTopicRequest = new CreateTopicRequest();
        createTopicRequest.setTopic(topic);
        createTopicRequest.setQueueNumber(queueNumber);
        TopicConfig tc = mqClientAPI.createTopic(createTopicRequest);
        if(tc != null){
            topicConfigTable.put(topic,tc);
        }
    }

    @Override
    public TopicConfig syncTopic(String topic) {
        return mqClientAPI.syncTopic(topic);
    }

    //异步这边给一个线程去运行，要不然就是用commonPool的ForkJoinPool
    protected <T> CompletableFuture<T> asyncSend(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier, sendExecutorService);
    }
}
