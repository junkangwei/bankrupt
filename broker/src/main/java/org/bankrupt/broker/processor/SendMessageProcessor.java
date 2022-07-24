package org.bankrupt.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.broker.commitLog.CommitLog;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.common.constant.HeaderConstant;
import org.bankrupt.common.constant.ResultEnums;
import org.bankrupt.common.exception.MQException;
import org.bankrupt.common.request.MessageAddRequest;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.constant.ProtocalConstant;
import org.bankrupt.remoting.common.process.Process;

import java.util.HashMap;
import java.util.Map;

public class SendMessageProcessor implements Process {

    private static Logger log = Logger.getLogger(CommitLog.class);

    private BrokerController brokerController;

    public SendMessageProcessor() {
    }

    public SendMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //处理具体的请求
    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = null;
        try {
            log.info("我是发送消息相关的");
            response = processRequest(ctx, remotingCommand);
        } catch (Exception e) {
            log.error("process SendMessage error, request : " + remotingCommand.toString(), e);
        }
        return response;
    }

    private RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        MessageAddRequest messageAddRequest = RemotingCommand.doDecode(remotingCommand.getBody(), MessageAddRequest.class);
        Map<String, String> header = messageAddRequest.getHeader();
        try{
            DefaultMessageStore messageStore = (DefaultMessageStore) brokerController.getMessageStore();
            CommitLog commitLog = messageStore.getCommitLog();
            //获得偏移量
            HashMap<String, Long> topicQueueTable = commitLog.getTopicQueueTable();
            RemotingCommand remotingCommand1 = brokerController.getRemoteServer().buildResponse(remotingCommand, ResultEnums.SEND_OK, -1);
            Message message = new Message();
            String topic = header.get(HeaderConstant.TOPIC);
            Integer queueId = Integer.valueOf(header.get(HeaderConstant.QUEUE_ID));
            if(header.containsKey(HeaderConstant.LEVEL)){
                header.put(HeaderConstant.REAL_QUEUE_ID,queueId.toString());
                header.put(HeaderConstant.REAL_TOPIC,topic);
                topic = HeaderConstant.SCHEDULE_TOPIC;
                queueId = Integer.parseInt(header.get(HeaderConstant.LEVEL)) - 1;
            }
            message.setBody(messageAddRequest.getBody());
            message.setTopic(topic);
            message.setBornTime(System.currentTimeMillis());
            message.setQueueId(queueId);
            String key = topic + "-" + queueId;
            Long queueOffset = topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                topicQueueTable.put(key, queueOffset);
            }
            message.setHeader(header.toString());
            message.setQueueOffset(queueOffset);
            commitLog.putMessage(message);
            remotingCommand1.setCode(ProtocalConstant.REMOTE_SUCCESS);
            remotingCommand1.setHeaders(header);
            return remotingCommand1;
        }catch (Exception e){
            return brokerController.getRemoteServer().buildError(remotingCommand, new MQException(e.getMessage()));
        }
    }
}
