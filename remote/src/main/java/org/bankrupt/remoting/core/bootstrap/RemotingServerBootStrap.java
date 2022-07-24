package org.bankrupt.remoting.core.bootstrap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.bankrupt.remoting.core.config.RemoteConfig;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 客户端实现类
 */
public class RemotingServerBootStrap extends AbstractBootStrap{
    /**
     * 启动类
     */
    ServerBootstrap serverBootstrap;
    /**
     * bossGroup
     */
    EventLoopGroup bossGroup;
    /**
     * workgroup
     */
    EventLoopGroup workGroup;

    public RemotingServerBootStrap(RemoteConfig config) {
        super(config);
    }

    @Override
    protected void doStart() throws InterruptedException {
        serverBootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(config.getBossThreads(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyNIOBoss_%d", this.threadIndex.incrementAndGet()));
            }
        });
        workGroup = new NioEventLoopGroup(config.getWorkThreads(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            private int threadTotal = config.getWorkThreads();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyServerNIOword_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
            }
        });
        serverBootstrap
                .group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(channelInitializer());
        ChannelFuture channelFuture = serverBootstrap.bind(config.getHost(), config.getPort()).sync();
        if (!channelFuture.isSuccess()) {
            throw new RuntimeException("server start error ", channelFuture.cause());
        }
    }

    @Override
    protected void doClose() throws InterruptedException {
        bossGroup.shutdownGracefully();
        workGroup.shutdownGracefully();
    }
}
