package com.bankrupt.client.producer;

import org.bankrupt.common.request.MessageAddRequest;
import org.bankrupt.remoting.common.RemoteLifeCycle;

import java.util.HashMap;

public interface MQClientAPI extends RemoteLifeCycle {

    String send(MessageAddRequest messageAddRequest);

    String send(MessageAddRequest messageAddRequest, HashMap<String,String> map);
}
