package org.bankrupt.broker;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.log4j.Logger;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.commitLog.MessageStore;
import org.bankrupt.broker.config.BrokerConfig;
import org.bankrupt.broker.processor.ConsumeOffsetProcessor;
import org.bankrupt.broker.processor.CreateTopicProcessor;
import org.bankrupt.broker.processor.PullMessageProcessor;
import org.bankrupt.broker.processor.SendMessageProcessor;
import org.bankrupt.broker.topic.DefaultTopicManage;
import org.bankrupt.common.LifeCycle;
import org.bankrupt.common.OffsetStore;
import org.bankrupt.common.constant.ProcessCodeConstant;
import org.bankrupt.common.exception.MQException;
import org.bankrupt.remoting.core.RemoteServer;
import org.bankrupt.remoting.core.config.RemoteConfig;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * broker主类
 */
public class BrokerController implements LifeCycle {
    public static Logger log = Logger.getLogger(BrokerController.class);

    //通信
    private RemoteServer remoteServer;
    //broker配置类
    private BrokerConfig brokerConfig;
    //处理topic
    private DefaultTopicManage topicManage;
    //消息
    private MessageStore messageStore;

    //线程池 消息消费的大一点
    private ExecutorService messageExecutorService;

    private ExecutorService publicExecutorService;

    private ExecutorService topicExecutorService;

    private ExecutorService offsetExecutorService;

    private ExecutorService pullExecutorService;

    private ScheduledExecutorService scheduledExecutorService;

    private AtomicBoolean brokerStart = new AtomicBoolean(false);

    private OffsetStore offsetStore;

    public BrokerController(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.topicManage = new DefaultTopicManage();
        this.offsetStore = new OffsetStore();
        this.messageStore = new DefaultMessageStore();
    }

    public RemoteServer getRemoteServer() {
        return remoteServer;
    }

    @Override
    public void start() {
        if (brokerStart.compareAndSet(false, true)) {
            try {
                init();
                doStart();
            } catch (Exception e) {
                throw new MQException("broker start error", e);
            }
        }
    }

    private void doStart() {
        this.messageStore.start();
        this.remoteServer.start();
        startExecutors();
    }

    private void startExecutors() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.offsetStore.persist();
                    BrokerController.this.topicManage.persist();
                } catch (Throwable e) {
                    log.error("schedule persist consumerOffset error.", e);
                }
            }
        }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

    }

    //初始化
    private void init() {
        initConfig();
        initExecutors();
        initRemoteServer();
    }

    private void initConfig() {
        //初始化消费进度
        offsetStore.load();
        topicManage.load();
    }

    private void initExecutors() {
        // message线程池要大一点,要写入数据和读取数据
        messageExecutorService = Executors.newFixedThreadPool(32, new DefaultThreadFactory("messageExecutorService_"));
        //通用的线程池
        publicExecutorService = Executors.newFixedThreadPool(16, new DefaultThreadFactory("publicExecutorService_"));
        //专门处理topic处理的
        topicExecutorService = Executors.newFixedThreadPool(4, new DefaultThreadFactory("topicExecutorService_"));
        //专门处理offset的
        offsetExecutorService = Executors.newFixedThreadPool(4, new DefaultThreadFactory("offsetExecutorService_"));
        //专门处理拉取请求的
        pullExecutorService = Executors.newFixedThreadPool(4, new DefaultThreadFactory("pullMessageExecutorService_"));
        //延时任务
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PeristScheduledThread");
            }
        });
    }

    private void initRemoteServer() {
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setPort(brokerConfig.getPort());
        remoteConfig.setHost("127.0.0.1");
        remoteServer = new RemoteServer(remoteConfig);
        registryProcess();
    }

    //注册处理器
    private void registryProcess() {
        /**
         * message相关
         */
        SendMessageProcessor sendMessageProcessor = new SendMessageProcessor(this);
        remoteServer.registerProcess(ProcessCodeConstant.SEND_MESSAGE, sendMessageProcessor, messageExecutorService);
        /**
         * topic相关
         */
        CreateTopicProcessor createTopicProcessor = new CreateTopicProcessor(this);
        remoteServer.registerProcess(ProcessCodeConstant.CREATE_TOPIC, createTopicProcessor, topicExecutorService);
        remoteServer.registerProcess(ProcessCodeConstant.SYNC_TOPIC, createTopicProcessor, topicExecutorService);
        /**
         * 消息消费相关
         */
        ConsumeOffsetProcessor consumeOffsetProcessor = new ConsumeOffsetProcessor(this);
        remoteServer.registerProcess(ProcessCodeConstant.QUERY_OFFSET, consumeOffsetProcessor, topicExecutorService);
        remoteServer.registerProcess(ProcessCodeConstant.UPDATE_OFFSET, consumeOffsetProcessor, topicExecutorService);

        /**
         * 拉取消息相关
         */
        PullMessageProcessor pullMessageProcessor = new PullMessageProcessor(this);
        remoteServer.registerProcess(ProcessCodeConstant.PULL_MESSAGE, pullMessageProcessor, pullExecutorService);
    }

    /**
     * 关闭broker
     */
    @Override
    public void close() {
        if (brokerStart.compareAndSet(true, false)) {
            messageStore.shutDown();
            messageExecutorService.shutdown();
            publicExecutorService.shutdown();
            topicExecutorService.shutdown();
            offsetExecutorService.shutdown();
            scheduledExecutorService.shutdown();
            this.offsetStore.persist();
            remoteServer.close();
        }
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public DefaultTopicManage getTopicManage() {
        return topicManage;
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public void setBrokerConfig(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }
}
