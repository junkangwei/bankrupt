package com.bankrupt.client;

import com.bankrupt.client.config.ClientConfig;
import config.TopicConfig;
import io.netty.channel.Channel;
import org.apache.log4j.Logger;
import org.bankrupt.common.constant.ProcessCodeConstant;
import org.bankrupt.common.request.*;
import org.bankrupt.common.response.MessagePullResponse;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.exception.RemoteException;
import org.bankrupt.remoting.core.RemoteClient;
import org.bankrupt.remoting.core.config.RemoteConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * 消息发送类
 */
public class MQClientAPIImpl implements MQClientAPI{

    public static Logger log = Logger.getLogger(MQClientAPIImpl.class);

    private ClientConfig clientConfig;

    private RemoteClient remoteClient;

    private Channel channel;

    public MQClientAPIImpl(ClientConfig config) {
        this.clientConfig = config;
    }

    @Override
    public void start() {
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setHost(clientConfig.getBrokerHost());
        remoteConfig.setPort(clientConfig.getBrokerPort());
        remoteClient = new RemoteClient(remoteConfig);
        remoteClient.start();
    }


    protected <T> T send(Object msg, int code, Class<T> clazz, Map<String,String> map) {
        try {
            RemotingCommand message = remoteClient.buildRequest(msg, code);
            message.setHeaders(map);
            log.info("返回的结果是:" + message);
            if (channel == null || !channel.isActive()) {
                synchronized (this) {
                    if (channel == null || !channel.isActive()) {
                        channel = remoteClient.getBootStrap().connect(clientConfig.getBrokerHost(), clientConfig.getBrokerPort());
                    }
                }
            }
            return remoteClient.send(channel, message, clazz);
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }

    protected <T> T send(Object msg, int code, Class<T> clazz) {
        try {
//            log.info("消息是:{},code是:{}",msg,code);
            RemotingCommand message = remoteClient.buildRequest(msg, code);
            log.info("返回的结果是:" + message);
            if (channel == null || !channel.isActive()) {
                synchronized (this) {
                    if (channel == null || !channel.isActive()) {
                        channel = remoteClient.getBootStrap().connect(clientConfig.getBrokerHost(), clientConfig.getBrokerPort());
                    }
                }
            }
            return remoteClient.send(channel, message, clazz);
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }

    @Override
    public void close() {
        remoteClient.close();
    }

    @Override
    public String send(MessageAddRequest messageAddRequest) {
        return send(messageAddRequest, ProcessCodeConstant.SEND_MESSAGE, String.class);
    }

    @Override
    public String send(MessageAddRequest messageAddRequest, Map<String, String> map) {
        return send(messageAddRequest, ProcessCodeConstant.SEND_MESSAGE, String.class,map);

    }

    @Override
    public TopicConfig createTopic(CreateTopicRequest createTopicRequest) {
        return send(createTopicRequest, ProcessCodeConstant.CREATE_TOPIC, TopicConfig.class);
    }

    @Override
    public TopicConfig syncTopic(String topic) {
        return send(topic, ProcessCodeConstant.SYNC_TOPIC, TopicConfig.class);
    }

    @Override
    public MessagePullResponse pullMessage(MessagePullRequest messagePullRequest) {
        return send(messagePullRequest, ProcessCodeConstant.PULL_MESSAGE, MessagePullResponse.class);
    }

    @Override
    public void commitOffset(OffsetCommitRequest offsetCommitRequest) {
        send(offsetCommitRequest, ProcessCodeConstant.UPDATE_OFFSET, String.class);
    }

    @Override
    public int query(OffsetQueryRequest offsetQueryRequest) {
        return send(offsetQueryRequest, ProcessCodeConstant.QUERY_OFFSET, Integer.class);
    }
}
