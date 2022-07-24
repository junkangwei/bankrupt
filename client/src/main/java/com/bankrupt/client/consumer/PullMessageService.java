package com.bankrupt.client.consumer;

import com.bankrupt.client.MQClientAPIImpl;
import com.bankrupt.client.config.ClientConfig;
import config.TopicConfig;
import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.common.LifeCycle;
import org.bankrupt.common.OffsetStore;
import org.bankrupt.common.request.MessagePullRequest;
import org.bankrupt.common.request.OffsetCommitRequest;
import org.bankrupt.common.request.OffsetQueryRequest;
import org.bankrupt.common.response.MessagePullResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PullMessageService implements LifeCycle {

    public static Logger log = Logger.getLogger(PullMessageService.class);

    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();

    private PullDefaultConsumer pullDefaultConsumer;

    private Map<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private OffsetStore offsetStore;

    private MessageListener messageListener;

    private ClientConfig clientConfig;

    public PullMessageService(PullDefaultConsumer pullDefaultConsumer) {
        this.pullDefaultConsumer = pullDefaultConsumer;
        this.offsetStore = new OffsetStore();
        clientConfig = pullDefaultConsumer.getConfig();
    }

    @Override
    public void start() {
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(topicConfigTable.size() * 4);
        for (Map.Entry<String, TopicConfig> stringTopicConfigEntry : topicConfigTable.entrySet()) {
            TopicConfig topicConfig = stringTopicConfigEntry.getValue();
            String topic = stringTopicConfigEntry.getKey();
            for (int i = 0; i < topicConfig.getQueueNumber(); i++) {
                PullRequest pullRequest = new PullRequest();
                pullRequest.setTopic(topic);
                pullRequest.setQueueId(i);
                scheduledThreadPoolExecutor.scheduleAtFixedRate(() ->
                        pullMessage(pullRequest, messageListener), 0, clientConfig.getPullMessageInterval(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public void close() {
        //上报一次所有的消息
        scheduledThreadPoolExecutor.shutdown();
    }

    /**
     * 拉取消息
     * @param pullRequest
     */
    private void pullMessage(PullRequest pullRequest,MessageListener messageListener) {
        try {
            MQClientAPIImpl mqClientAPI = this.pullDefaultConsumer.getMqClientAPI();
            MessagePullRequest messagePullRequest = new MessagePullRequest();
            String topic = pullRequest.getTopic();
            Integer queueId = pullRequest.getQueueId();
            messagePullRequest.setTopic(topic);
            messagePullRequest.setQueueId(queueId);
            messagePullRequest.setLoad(true);//每次都持久化一次
            Integer offset = offsetStore.getNextOffset(topic, queueId);
            if(offset == null){
                //去broker拿一次
                OffsetQueryRequest offsetQueryRequest = new OffsetQueryRequest();
                offsetQueryRequest.setQueueId(queueId);
                offsetQueryRequest.setTopic(topic);
                offset = mqClientAPI.query(offsetQueryRequest);
                this.offsetStore.updateOffset(topic, queueId,offset);
            }
            messagePullRequest.setOffset(offset);
            MessagePullResponse messagePullResponse = mqClientAPI.pullMessage(messagePullRequest);
            List<Message> messageList = messagePullResponse.getMessageList();
            if(messageList != null && messageList.size() > 0){
                int currentOffset = messageList.get(0).getOffset();;
                int commitOffset = -1;
                int count = 0;
                try {
                    for (Message message : messageList) {
                        if(messageListener.listener(message)){
                            commitOffset = message.getNextOffset();
                        }else{
                            //如果异常直接break,拿着这个offset去哪里拿消息
                            commitOffset = message.getOffset();
                            break;
                        }
                        count++;
                    }
                }catch (Exception e){
                    commitOffset = messageList.get(count).getOffset();
                }finally {
                    if (currentOffset != commitOffset) {
                        //更新本地的消费进度
                        this.offsetStore.updateOffset(topic, queueId,commitOffset);
                        OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest();
                        offsetCommitRequest.setTopic(topic);
                        offsetCommitRequest.setQueueId(queueId);
                        offsetCommitRequest.setCurrentOffset(currentOffset);
                        offsetCommitRequest.setCommitOffset(commitOffset);
                        pullDefaultConsumer.getMqClientAPI().commitOffset(offsetCommitRequest);
                    }
                }
            }
        }catch (Exception e){
            log.error("", e);
        }

    }


    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public void setTopicConfigTable(Map<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }
}
