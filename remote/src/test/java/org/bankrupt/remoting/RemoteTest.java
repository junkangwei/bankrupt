package org.bankrupt.remoting;

import io.netty.channel.ChannelHandlerContext;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.process.Process;
import org.bankrupt.remoting.core.RemoteClient;
import org.bankrupt.remoting.core.RemoteServer;
import org.bankrupt.remoting.core.config.RemoteConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class RemoteTest {
    RemoteServer remoteServer;
    RemoteClient remoteClient;
    RemoteConfig remoteConfig;

    @Before
    public void before() {
        remoteConfig = new RemoteConfig();
        remoteConfig.setHost("localhost");
        remoteConfig.setPort(9000);
        remoteConfig.setWorkThreads(8);
        remoteConfig.setBossThreads(1);
        remoteServer = new RemoteServer(remoteConfig);
        remoteClient = new RemoteClient(remoteConfig);
    }

    @Test
    public void echoTest() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        remoteServer.registerProcess(1, (ctx, req) -> {
            User user = RemotingCommand.doDecode(req.getBody(),User.class);
            System.out.println(user);
            countDownLatch.countDown();
            return req;
        }, Executors.newSingleThreadExecutor());
        remoteServer.start();
        remoteClient.start();
        remoteClient.send(remoteConfig.getHost(), remoteConfig.getPort(), remoteClient.buildRequest(new User("张三", 18), 1));
        countDownLatch.await();
    }


    @Test
    public void sendResponseTest() {
        remoteServer.registerProcess(1, new Process() {
            @Override
            public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand req) {
                ctx.writeAndFlush(remoteServer.buildResponse(req, "Hello World :", 1));
                return req;
            }
        }, Executors.newSingleThreadExecutor());
        remoteServer.start();
        remoteClient.start();
        String response = remoteClient.send(remoteConfig.getHost(), remoteConfig.getPort(), remoteClient.buildRequest(new User("张三", 18), 1), String.class);
        System.out.println(response);
    }

    @Test
    public void errorTest() {
        remoteServer.registerProcess(1, (ctx, req) -> {
            ctx.writeAndFlush(remoteServer.buildError(req, new NullPointerException("该数据不存在")));
            return req;
        }, Executors.newSingleThreadExecutor());
        remoteServer.start();
        remoteClient.start();
        try {
            String response = remoteClient.send(remoteConfig.getHost(), remoteConfig.getPort(), remoteClient.buildRequest(new User("张三", 18), 1), String.class);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        remoteClient.close();
        remoteServer.close();
    }
}
