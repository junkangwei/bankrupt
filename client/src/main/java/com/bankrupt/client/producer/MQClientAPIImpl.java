package com.bankrupt.client.producer;

import com.bankrupt.client.config.ClientConfig;
import io.netty.channel.Channel;
import org.bankrupt.common.constant.ProcessCodeConstant;
import org.bankrupt.common.request.MessageAddRequest;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.exception.RemoteException;
import org.bankrupt.remoting.core.RemoteClient;
import org.bankrupt.remoting.core.config.RemoteConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * 消息发送类
 */
public class MQClientAPIImpl implements MQClientAPI{

    public static Logger log = LoggerFactory.getLogger(MQClientAPIImpl.class);

    private ClientConfig clientConfig;

    private RemoteClient remoteClient;

    private Channel channel;

    private Runnable runnable;

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


    protected <T> T send(Object msg, int code, Class<T> clazz,HashMap<String,String> map) {
        try {
            log.info("消息是:{},code是:{}",msg,code);
            RemotingCommand message = remoteClient.buildRequest(msg, code);
            message.setHeaders(map);
            log.info("返回的结果是:{}",message);
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
            log.info("消息是:{},code是:{}",msg,code);
            RemotingCommand message = remoteClient.buildRequest(msg, code);
            log.info("返回的结果是:{}",message);
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
    public String send(MessageAddRequest messageAddRequest, HashMap<String, String> map) {
        return send(messageAddRequest, ProcessCodeConstant.SEND_MESSAGE, String.class,map);

    }
}
