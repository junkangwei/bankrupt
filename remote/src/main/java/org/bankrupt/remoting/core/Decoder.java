package org.bankrupt.remoting.core;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.bankrupt.remoting.common.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 解码
 */
public class Decoder extends LengthFieldBasedFrameDecoder {

    public Decoder() {
        super(1024 * 1024 * 10, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame =(ByteBuf) super.decode(ctx, in);
        if (null == frame) {
            return null;
        }

        ByteBuffer byteBuffer = frame.nioBuffer();
        RemotingCommand decode = RemotingCommand.decode(byteBuffer);
        frame.release();
        return decode;
    }
}
