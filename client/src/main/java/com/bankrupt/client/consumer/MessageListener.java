package com.bankrupt.client.consumer;


import message.Message;

/**
 * 消息处理器
 */
public interface MessageListener {

    boolean listener(Message message);
}
