package org.bankrupt.remoting.core;

import io.netty.channel.ChannelHandler;
import org.bankrupt.remoting.core.bootstrap.RemotingServerBootStrap;
import org.bankrupt.remoting.core.config.RemoteConfig;

public class RemoteServer extends AbstractRemote<RemotingServerBootStrap>{

    protected RemotingServerBootStrap remotingServerBootStrap;

    private ChannelHandler channelHandler = new ServerHandler();

    public RemoteServer(RemoteConfig config) {
        super(config);
    }

    @Override
    public void start() {
        remotingServerBootStrap = new RemotingServerBootStrap(getConfig()) {
            @Override
            public ChannelHandler[] getHandlerArray() {
                return new ChannelHandler[]{channelHandler};
            }
        };
        super.start();
    }

    @Override
    protected RemotingServerBootStrap getBootStrap() {
        return remotingServerBootStrap;
    }

    @ChannelHandler.Sharable
    public class ServerHandler extends AbstractHandler {

    }
}
