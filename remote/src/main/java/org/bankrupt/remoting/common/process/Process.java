package org.bankrupt.remoting.common.process;


import io.netty.channel.ChannelHandlerContext;
import org.bankrupt.remoting.common.RemotingCommand;

/**
 * 请求处理
 */
@FunctionalInterface
public interface Process {
    /**
     * 处理消息
     *
     * @param ctx     上下文
     * @param remotingCommand 信息,body自己decode到自己所需要的类型
     */
    RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand);

}
