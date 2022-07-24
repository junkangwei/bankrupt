package org.bankrupt.broker.processor;

import config.TopicConfig;
import io.netty.channel.ChannelHandlerContext;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.broker.topic.DefaultTopicManage;
import org.bankrupt.common.constant.ProcessCodeConstant;
import org.bankrupt.common.constant.ResultEnums;
import org.bankrupt.common.exception.MQException;
import org.bankrupt.common.request.CreateTopicRequest;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.constant.ProtocalConstant;
import org.bankrupt.remoting.common.process.Process;
import org.apache.log4j.Logger;

public class CreateTopicProcessor implements Process {

    public static Logger log = Logger.getLogger(CreateTopicProcessor.class);

    private BrokerController brokerController;

    public CreateTopicProcessor() {
    }

    public CreateTopicProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //处理具体的请求
    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = null;
        try {
            switch (remotingCommand.getCode()){
                case ProcessCodeConstant.CREATE_TOPIC:
                    log.info("我是创建主题相关的");
                    response = processRequest(ctx, remotingCommand);
                    break;
                case ProcessCodeConstant.SYNC_TOPIC:
                    log.info("我是同步主题相关的");
                    response = syncTopicRequest(ctx, remotingCommand);
                    break;
            }
        } catch (Exception e) {
            log.error("process SendMessage error, request : " + remotingCommand.toString(), e);
        }
        return response;
    }

    private RemotingCommand syncTopicRequest(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = brokerController.getRemoteServer().buildResponse(remotingCommand, ResultEnums.CREATE_OK, -1);
        String topic = RemotingCommand.doDecode(remotingCommand.getBody(), String.class);
        DefaultTopicManage topicManage = brokerController.getTopicManage();
        TopicConfig topicConfig = topicManage.getTopic(topic);
        response.setBody(RemotingCommand.encode(topicConfig));
        response.setCode(ProtocalConstant.REMOTE_SUCCESS);
        return response;
    }

    private RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = brokerController.getRemoteServer().buildResponse(remotingCommand, ResultEnums.CREATE_OK, -1);
        CreateTopicRequest createTopicRequest = RemotingCommand.doDecode(remotingCommand.getBody(), CreateTopicRequest.class);
        String topic = createTopicRequest.getTopic();
        int queueNumber = createTopicRequest.getQueueNumber();
        DefaultTopicManage topicManage = brokerController.getTopicManage();
        if(topicManage.exists(topic)){
            TopicConfig topicConfig = topicManage.createTopic(topic, queueNumber);
            response.setBody(RemotingCommand.encode(topicConfig));
        }else{
            try {
                TopicConfig topicConfig = topicManage.createTopic(topic, queueNumber);
                response.setBody(RemotingCommand.encode(topicConfig));
            } catch (Exception e) {
                log.error("topic create error", e);
                return brokerController.getRemoteServer().buildError(remotingCommand,new MQException("create topic error"));
            }
        }
        response.setCode(ProtocalConstant.REMOTE_SUCCESS);
        return response;
    }
}
