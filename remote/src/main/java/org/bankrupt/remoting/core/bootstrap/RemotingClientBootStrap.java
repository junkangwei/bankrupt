package org.bankrupt.remoting.core.bootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.bankrupt.remoting.common.exception.RemoteException;
import org.bankrupt.remoting.core.config.RemoteConfig;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务端启动类
 */
public class RemotingClientBootStrap extends AbstractBootStrap{

    Bootstrap bootstrap;

    EventLoopGroup workGroup;

    Map<SocketAddress, Channel> socketAddressChannelMap = new ConcurrentHashMap<>();

    public RemotingClientBootStrap(RemoteConfig config) {
        super(config);
    }

    @Override
    public void doStart() {
        bootstrap = new Bootstrap();

        workGroup = new NioEventLoopGroup(config.getWorkThreads());

        bootstrap
                .group(workGroup)
                .channel(NioSocketChannel.class)
                .handler(channelInitializer());
    }

    @Override
    protected void doClose() throws InterruptedException {
        workGroup.shutdownGracefully();
    }

    public Channel connect(String host, int port) {
        return connect(InetSocketAddress.createUnresolved(host, port));
    }

    public Channel connect(SocketAddress address) {
        Channel channel = socketAddressChannelMap.get(address);
        if (channel == null || (channel != null && !channel.isActive())) {
            synchronized (this) {
                channel = socketAddressChannelMap.get(address);
                if (channel == null || (channel != null && !channel.isActive())) {
                    channel = doConnect(address);
                    socketAddressChannelMap.put(address, channel);
                }
            }
        }
        return channel;

    }


    private Channel doConnect(SocketAddress remoteAddress) {
        try {
            ChannelFuture connectFuture = bootstrap.connect(remoteAddress).sync();
            if (!connectFuture.isSuccess()) {
                throw new RemoteException(connectFuture.cause());
            }
            return connectFuture.channel();
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }
}
