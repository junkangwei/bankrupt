package com.bankrupt.client;

import config.TopicConfig;
import org.bankrupt.common.request.*;
import org.bankrupt.common.response.MessagePullResponse;
import org.bankrupt.remoting.common.RemoteLifeCycle;

import java.util.HashMap;
import java.util.Map;

public interface MQClientAPI extends RemoteLifeCycle {

    String send(MessageAddRequest messageAddRequest);

    String send(MessageAddRequest messageAddRequest, Map<String,String> map);

    TopicConfig createTopic(CreateTopicRequest createTopicRequest);

    TopicConfig syncTopic(String topic);

    MessagePullResponse pullMessage(MessagePullRequest messagePullRequest);

    void commitOffset(OffsetCommitRequest offsetCommitRequest);

    int query(OffsetQueryRequest offsetQueryRequest);
}
