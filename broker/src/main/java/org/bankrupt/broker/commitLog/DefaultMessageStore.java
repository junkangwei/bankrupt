package org.bankrupt.broker.commitLog;

import message.Message;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.broker.delay.DelayMessageService;
import org.bankrupt.broker.store.MessageGetResult;
import org.apache.log4j.Logger;
import org.bankrupt.broker.store.*;
import org.bankrupt.broker.topic.ConsumeQueue;
import org.bankrupt.broker.topic.DefaultTopicManage;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.*;

public class DefaultMessageStore implements MessageStore{

    private static Logger log = Logger.getLogger(DefaultMessageStore.class);

    public static Logger logger = Logger.getLogger(DefaultMessageStore.class);

    private final MessageStoreConfig messageStoreConfig;

    private final CommitLog commitLog;

    private ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable = new ConcurrentHashMap<>(16);

    private CommitLogDispatcher commitLogDispatcher;

    private ReputMessageService reputMessageService;

    private DelayMessageService delayMessageService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "FlushScheduledThread");
        }
    });

    public DefaultMessageStore() {
        this.messageStoreConfig = new MessageStoreConfig();
        this.commitLogDispatcher = new ConsumeQueueDispatcher(this);
        this.reputMessageService = new ReputMessageService(this);
        this.commitLog = new CommitLog(this);
        this.delayMessageService = new DelayMessageService(this);
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }


    @Override
    public boolean load() {
        boolean result = true;
        //commitLog加载
        result = this.commitLog.load();
        //consumequeue的加载
        result = result && this.loadConsumeQueue();
        //初始化文件，把消息放进去
        result = result && initOffset();
        return result;
    }

    /**
     * 初始化commitLog的put进度 && 初始化consumeQueue的put进度
     * @return
     */
    private boolean initOffset() {
        HashMap<String, Long> table = new HashMap<>();
        boolean result = true;
        //先恢复comsumeQueue
        result = result && this.loadQueueData(table);
        //在恢复commitLog
        if(result){
            long offset = this.commitLog.loadData();
            //设置当前的消费进度
            this.reputMessageService.setReputFromOffset(offset);
        }
        this.commitLog.setTopicQueueTable(table);
        return result;
    }

    //恢复consumequeue
    private boolean loadQueueData(HashMap<String, Long> table) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> consumeQueueTable = this.consumeQueueTable;
        if(this.consumeQueueTable != null){
            for (String key : consumeQueueTable.keySet()) {
                ConcurrentMap<Integer, ConsumeQueue> integerConsumeQueueConcurrentMap = consumeQueueTable.get(key);
                for (ConsumeQueue cq : integerConsumeQueueConcurrentMap.values()) {
                    int recover = cq.recover();
                    table.put(key + "-" + 0, (long) recover);
                }
            }
        }
        return true;
    }

    @Override
    public void start() {
        boolean load = load();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    commitLog.flush();;
                } catch (Throwable e) {
                    log.error("schedule persist consumerOffset error.", e);
                }
            }
        }, 1000 * 5, this.messageStoreConfig.getFlushCommitLog(), TimeUnit.MILLISECONDS);
        if(load){
            this.reputMessageService.start();
            this.delayMessageService.start();
        }
    }

    @Override
    public void shutDown() {
        this.reputMessageService.shutdown();
        this.delayMessageService.close();
        commitLog.flush();
    }

    @Override
    public MessageGetResult getMessage(int offset, int queueId, String topic, Boolean load) {
        long nextOffset = (long) offset;
        long minOffset = 0;
        long maxOffset = 0;
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        MessageGetResult messageGetResult = new MessageGetResult();
        if(consumeQueue != null){
            minOffset = consumeQueue.getMinOffset();
            maxOffset = consumeQueue.getMaxOffsetInQueue();//消费的数量 988
            if(offset == maxOffset){
                //说明没有消息
            }else if(offset > maxOffset){
                //说明本地消费完，没有上报
            }else {
                //说明有消息
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getMoreMessage(offset);
                int i = 0;
                try {
                    int count = 0;
                    for(;i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE){
                        long cmOffset = bufferConsumeQueue.getByteBuffer().getLong();
                        int size = bufferConsumeQueue.getByteBuffer().getInt();
                        long storeTime = bufferConsumeQueue.getByteBuffer().getLong();
                        Message message = this.commitLog.getMessage(cmOffset, size);
                        if(message != null){
                            count++;
                            message.setOffset(message.getQueueOffset().intValue());
                            message.setNextOffset(message.getOffset() + 1);
                            messageGetResult.addMessage(message);
                            messageGetResult.setFound(Boolean.TRUE);
                        }
                    }
                    nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                }catch (Exception e){
                    logger.error("获取消息发生异常当前的i:" + i);
                }
                logger.info("获得到的消息的数量是:" + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE));
            }
        }
        messageGetResult.setNextBeginOffset(nextOffset);
        messageGetResult.setMaxOffset(maxOffset);
        messageGetResult.setMinOffset(minOffset);
        return messageGetResult;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        if(this.consumeQueueTable == null){
            consumeQueueTable = new ConcurrentHashMap<>();
        }
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        //如果没有初始化
        if(map == null){
            logger.info("当前创建consumeQueue的topic是:" +topic);
            ConcurrentMap<Integer, ConsumeQueue> map1 = new ConcurrentHashMap<Integer, ConsumeQueue>(16);
            consumeQueueTable.put(topic, map1);
            map = consumeQueueTable.get(topic);
        }
        ConsumeQueue consumeQueue = map.get(queueId);
        //新建的如果没有
        if(consumeQueue == null){
            logger.info("当前创建consumeQueue的queue是:" + queueId);
            ConsumeQueue newConsumeQueue = new ConsumeQueue(topic, queueId,
                    this.messageStoreConfig.getStorePathConsumeQueue(),
                    this.messageStoreConfig.getMappedFileSizeConsumeQueue(), this);
            map.putIfAbsent(queueId, newConsumeQueue);
            consumeQueue = map.get(queueId);
        }
        return consumeQueue;
    }

    private boolean loadConsumeQueue() {
        File dir = new File(this.messageStoreConfig.getStorePathConsumeQueue());
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File topicFile : files) {
                String topic = topicFile.getName();

                File[] topicQueueList = topicFile.listFiles();
                //具体的
                if(topicQueueList != null && topicQueueList.length > 0){
                    for (File file : topicQueueList) {
                        int queueId;
                        try {//获得queue的队列id
                            queueId = Integer.parseInt(file.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }//生成ConsumeQueue
                        ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId,
                                this.messageStoreConfig.getStorePathConsumeQueue(),
                                this.messageStoreConfig.getMappedFileSizeConsumeQueue(), this);
                        this.putConsumeQueue(topic,queueId,consumeQueue);
                    }
                }
            }
        }
        return true;
    }

    public void doDispatch(DispatchRequest dispatchRequest) {
        commitLogDispatcher.doDispatch(dispatchRequest);
    }



}
