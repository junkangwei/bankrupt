package org.bankrupt.remoting.core;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.bankrupt.remoting.common.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 编码
 */
public class Encoder extends MessageToByteEncoder<RemotingCommand> {

    /**
     * 写消息序列化
     * @param ctx
     * @param remotingCommand
     * @param out
     * @throws Exception
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
        byte[] header = remotingCommand.encodeHeaders();
        int totalLength = RemotingCommand.FIX_COMMON_LENGTH + header.length + remotingCommand.getBody().length;
        out.writeInt(totalLength);//4
        out.writeInt(remotingCommand.getId());//4
        out.writeInt(remotingCommand.getCode());//4
        out.writeByte(remotingCommand.getProtocol());//1
        out.writeByte(remotingCommand.getDirection());//1
        out.writeInt(header.length);//header的长度//4
        out.writeBytes(header);//header
        byte[] body = remotingCommand.getBody();
        if (body != null) {
            out.writeBytes(body);
        }
    }

}
