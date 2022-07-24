package org.bankrupt.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.common.constant.ProcessCodeConstant;
import org.bankrupt.common.constant.ResultEnums;
import org.bankrupt.common.request.OffsetCommitRequest;
import org.bankrupt.common.request.OffsetQueryRequest;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.constant.ProtocalConstant;
import org.bankrupt.remoting.common.process.Process;
import org.apache.log4j.Logger;

public class ConsumeOffsetProcessor implements Process {

    public static Logger log = Logger.getLogger(ConsumeOffsetProcessor.class);

    private BrokerController brokerController;

    public ConsumeOffsetProcessor() {
    }

    public ConsumeOffsetProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //处理具体的请求
    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = null;
        try {
            switch (remotingCommand.getCode()){
                case ProcessCodeConstant.QUERY_OFFSET:
                    response = processRequestQuery(ctx, remotingCommand);
                    break;
                case ProcessCodeConstant.UPDATE_OFFSET:
                    response = processRequestUpdate(ctx, remotingCommand);
                    break;
            }
        } catch (Exception e) {
            log.error("process SendMessage error, request : " + remotingCommand.toString(), e);
        }
        return response;
    }

    /**
     * 更新commmitSet
     * @param ctx
     * @param remotingCommand
     * @return
     */
    private RemotingCommand processRequestUpdate(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = brokerController.getRemoteServer().buildResponse(remotingCommand, ResultEnums.UPDATE_OFFSET_OK, -1);
        OffsetCommitRequest offsetCommitRequest = RemotingCommand.doDecode(remotingCommand.getBody(), OffsetCommitRequest.class);
        brokerController.getOffsetStore().updateOffset(offsetCommitRequest.getTopic(),offsetCommitRequest.getQueueId(),offsetCommitRequest.getCommitOffset());
        response.setHeaders(remotingCommand.getHeaders());
        response.setCode(ProtocalConstant.REMOTE_SUCCESS);
        return response;
    }


    private RemotingCommand processRequestQuery(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RemotingCommand response = brokerController.getRemoteServer().buildResponse(remotingCommand, ResultEnums.UPDATE_OFFSET_OK, -1);
        OffsetQueryRequest offsetQueryRequest = RemotingCommand.doDecode(remotingCommand.getBody(), OffsetQueryRequest.class);
        Integer nextOffset = brokerController.getOffsetStore().getNextOffset(offsetQueryRequest.getTopic(), offsetQueryRequest.getQueueId());
        response.setCode(ProtocalConstant.REMOTE_SUCCESS);
        response.setHeaders(remotingCommand.getHeaders());
        response.setBody(RemotingCommand.encode(nextOffset == null ? 0 : nextOffset));
        return response;
    }
}
