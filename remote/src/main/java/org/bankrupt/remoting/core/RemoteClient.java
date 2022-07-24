package org.bankrupt.remoting.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.core.bootstrap.RemotingClientBootStrap;
import org.bankrupt.remoting.core.config.RemoteConfig;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RemoteClient extends AbstractRemote<RemotingClientBootStrap>{

    private RemotingClientBootStrap remotingClientBootStrap;

    private ChannelHandler channelHandler = new ClientHandler();

    public RemoteClient(RemoteConfig config) {
        super(config);
    }

    @Override
    public RemotingClientBootStrap getBootStrap() {
        return remotingClientBootStrap;
    }

    @Override
    public void start() {
        remotingClientBootStrap = new RemotingClientBootStrap(getConfig()) {
            @Override
            public ChannelHandler[] getHandlerArray() {
                return new ChannelHandler[]{channelHandler};
            }
        };
        super.start();
    }

    public <T> T send(String host, int port, RemotingCommand message, Class<T> clazz) {
        return send(getChannel(host, port), message, clazz);
    }

    public <T> T send(String host, int port, RemotingCommand message, Class<T> clazz, long timeout, TimeUnit timeUnit) {
        return send(getChannel(host, port), message, clazz, timeout, timeUnit);
    }

    public <T> T send(SocketAddress socketAddress, RemotingCommand message, Class<T> clazz) {
        return send(getChannel(socketAddress), message, clazz);
    }

    public <T> T send(SocketAddress socketAddress, RemotingCommand message, Class<T> clazz, long timeout, TimeUnit timeUnit) {
        return send(getChannel(socketAddress), message, clazz, timeout, timeUnit);
    }

    public CompletableFuture<RemotingCommand> send(String host, int port, RemotingCommand message) {
        return asyncSend(getChannel(host, port), message).getRespFuture();
    }

    public CompletableFuture<RemotingCommand> send(SocketAddress socketAddress, RemotingCommand message) {
        return asyncSend(getChannel(socketAddress), message).getRespFuture();
    }

    private Channel getChannel(String host, int port) {
        return remotingClientBootStrap.connect(host, port);
    }

    private Channel getChannel(SocketAddress socketAddress) {
        return remotingClientBootStrap.connect(socketAddress);
    }



    @ChannelHandler.Sharable
    public class ClientHandler extends AbstractHandler {

    }
}
