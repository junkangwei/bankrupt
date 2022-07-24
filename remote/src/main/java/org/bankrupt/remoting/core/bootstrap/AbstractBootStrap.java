package org.bankrupt.remoting.core.bootstrap;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import org.bankrupt.remoting.common.RemoteLifeCycle;
import org.bankrupt.remoting.core.Decoder;
import org.bankrupt.remoting.core.Encoder;
import org.bankrupt.remoting.core.config.RemoteConfig;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 启动基础类
 */
public abstract class AbstractBootStrap implements RemoteLifeCycle {
    /**
     * 启动使用，防止重复启动
     */
    protected AtomicBoolean start = new AtomicBoolean(false);

    protected RemoteConfig config;

    public AbstractBootStrap(RemoteConfig config) {
        this.config = config;
    }


    public AtomicBoolean getStart() {
        return start;
    }

    public RemoteConfig getConfig() {
        return config;
    }

    /**
     * 启动
     */
    @Override
    public void start() {
        if(start.compareAndSet(false,true)){
            try {
                /**
                 * 启动客户端/服务端，具体让实现类去时间
                 */
                doStart();
            } catch (Exception e) {
                start.set(false);
                throw new RuntimeException(e);
            }
        }else{
            throw new RuntimeException("启动失败");
        }
    }

    /**
     * 让子类去实现内部的功能
     */
    protected abstract void doStart() throws InterruptedException;

    /**
     * 让子类去实现内部的功能
     */
    protected abstract void doClose() throws InterruptedException;

    protected ChannelHandler[] getHandlerArray() {
        return new ChannelHandler[0];
    }

    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch
                        .pipeline()
                        .addLast(
                                new Decoder(),new Encoder()
                        ).addLast(
                        getHandlerArray()
                );
            }
        };
    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            try {
                doClose();
            } catch (Exception e) {
                start.set(true);
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("it has been closed");
        }
    }
}
