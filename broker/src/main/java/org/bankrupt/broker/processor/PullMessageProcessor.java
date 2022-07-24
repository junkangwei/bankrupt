package org.bankrupt.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;
import org.bankrupt.broker.store.MessageGetResult;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.common.constant.ResultEnums;
import org.bankrupt.common.request.MessagePullRequest;
import org.bankrupt.common.response.MessagePullResponse;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.constant.ProtocalConstant;
import org.bankrupt.remoting.common.process.Process;

public class PullMessageProcessor implements Process {

    public static Logger log = Logger.getLogger(PullMessageProcessor.class);

    private BrokerController brokerController;

    public PullMessageProcessor() {
    }

    public PullMessageProcessor(BrokerController brokerController) {
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
        RemotingCommand response = brokerController.getRemoteServer().buildResponse(remotingCommand, ResultEnums.UPDATE_OFFSET_OK, -1);
        MessagePullRequest messagePullRequest = RemotingCommand.doDecode(remotingCommand.getBody(), MessagePullRequest.class);
        MessagePullResponse messagePullResponse = new MessagePullResponse();
        MessageGetResult messageGetResult = this.brokerController.getMessageStore()
                .getMessage(messagePullRequest.getOffset(),messagePullRequest.getQueueId(),messagePullRequest.getTopic(),messagePullRequest.getLoad());
        messagePullResponse.setMessageList(messageGetResult.getMessageList());
        messagePullResponse.setTopic(messagePullRequest.getTopic());
        messagePullResponse.setQueueId(messagePullRequest.getQueueId());
        messagePullResponse.setNextOffset(messageGetResult.getNextBeginOffset());
        response.setBody(RemotingCommand.encode(messagePullResponse));
        response.setCode(ProtocalConstant.REMOTE_SUCCESS);
        return response;
    }
}
