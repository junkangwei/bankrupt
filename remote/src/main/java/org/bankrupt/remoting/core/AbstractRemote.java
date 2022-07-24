package org.bankrupt.remoting.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import javafx.util.Pair;
import org.bankrupt.remoting.common.CommandFuture;
import org.bankrupt.remoting.common.RemotingCommand;
import org.bankrupt.remoting.common.RemoteLifeCycle;
import org.bankrupt.remoting.common.constant.DirectionConstant;
import org.bankrupt.remoting.common.constant.ProtocalConstant;
import org.bankrupt.remoting.common.exception.RemoteException;
import org.bankrupt.remoting.common.exception.RemoteExceptionBody;
import org.bankrupt.remoting.core.bootstrap.AbstractBootStrap;
import org.bankrupt.remoting.core.config.RemoteConfig;
import org.bankrupt.remoting.common.process.Process;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 客户端抽象类
 * @param <T>
 */
public abstract class AbstractRemote<T extends AbstractBootStrap> implements RemoteLifeCycle {
    protected final Map<Integer, Pair<Process, ExecutorService>> codeProcessMap = new HashMap<>();

    protected final Map<Integer, CommandFuture> futureMap = new ConcurrentHashMap<>();

    protected final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    private final RemoteConfig config;

    protected AbstractRemote(RemoteConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        getBootStrap().start();
        scheduledExecutorService.scheduleAtFixedRate(() -> futureTimeOut(), 1000, 1000, TimeUnit.MILLISECONDS);
    }

    protected abstract T getBootStrap();

    protected void futureTimeOut() {
        futureMap.forEach((id, message) -> {
            if (message.isTimeOut()) {
                message.getRespFuture().completeExceptionally(new TimeoutException("time limit exceeded"));
                futureMap.remove(id);
            }
        });
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
        getBootStrap().close();
    }

    public RemoteConfig getConfig() {
        return config;
    }

    public void registerProcess(int code, Process process, ExecutorService executorService) {
        codeProcessMap.put(code, new Pair(process, executorService));
    }

    public RemotingCommand buildRequest(Object msg, int code) {
        return buildRequest(msg, code , ProtocalConstant.DEFAULT_PROTOCOL);
    }

    public RemotingCommand buildRequest(Object msg, int code,Map<String, String> headers) {
        return buildRequest(msg, code , ProtocalConstant.DEFAULT_PROTOCOL,headers);
    }

    /**
     * 构建消息体
     * @param msg
     * @param code
     * @param protocol
     * @return
     */
    public RemotingCommand buildRequest(Object msg, int code, byte protocol) {
        return buildRequest(msg,code,protocol,new HashMap<>());
    }

    /**
     * 构建消息体
     * @param msg
     * @param code
     * @param protocol
     * @return
     */
    public RemotingCommand buildRequest(Object msg, int code, byte protocol,Map<String, String> headers) {
        RemotingCommand request = new RemotingCommand();
        request.setId(RemotingCommand.createRequestId());
        request.setCode(code);
        request.setProtocol(protocol);
        request.setHeaders(headers);
        request.setBody(RemotingCommand.encode(msg));
        request.setDirection(DirectionConstant.REQUEST);
        return request;
    }

    /**
     * 构建返回值
     * @param request
     * @param msg
     * @param code
     * @return
     */
    public RemotingCommand buildResponse(RemotingCommand request, Object msg, int code) {
        RemotingCommand response = new RemotingCommand();
        response.setId(request.getId());
        response.setCode(code);
        response.setProtocol(request.getProtocol());
        response.setBody(RemotingCommand.encode(msg));
        response.setDirection(DirectionConstant.RESPONSE);
        return response;
    }

    public <T> T send(Channel channel, RemotingCommand remotingCommand, Class<T> clazz) {
        return send(channel, remotingCommand, clazz, getConfig().getTimeOut(), TimeUnit.MILLISECONDS);
    }

    /**
     * 同步发送，并等待响应
     */
    public <T> T send(Channel channel, RemotingCommand remotingCommand, Class<T> clazz, long timeOut, TimeUnit timeUnit) {
        try {
            RemotingCommand reponse = asyncSend(channel, remotingCommand).getRespFuture().get(timeOut, timeUnit);
            if(reponse == null){
                return null;
            }
            return RemotingCommand.doDecode(reponse.getBody(), clazz);
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }

    public CommandFuture asyncSend(Channel channel, RemotingCommand remotingCommand) {
        CommandFuture messageFuture = new CommandFuture(remotingCommand, getConfig().getTimeOut(), TimeUnit.MILLISECONDS);
        futureMap.put(remotingCommand.getId(), messageFuture);
        ChannelFuture channelFuture = channel.writeAndFlush(remotingCommand);
        channelFuture.addListener((future -> {
            if (!future.isSuccess()) {
                futureMap.remove(remotingCommand.getId());
                messageFuture.getRespFuture().completeExceptionally(channelFuture.cause());
            }
        }));
        return messageFuture;
    }

    protected void doProcess(ChannelHandlerContext ctx, RemotingCommand req) {
        processResp(ctx, req);
        Pair<Process, ExecutorService> requestProcessEventLoopGroupPair = codeProcessMap.get(req.getCode());
        if (requestProcessEventLoopGroupPair != null) {
            Process process = requestProcessEventLoopGroupPair.getKey();
            ExecutorService eventLoopGroup = requestProcessEventLoopGroupPair.getValue();
            eventLoopGroup.execute(() -> {
                RemotingCommand remotingCommand = process.process(ctx, req);
                try {
                    ctx.writeAndFlush(remotingCommand);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * 结果处理
     * @param ctx
     * @param msg
     */
    protected void processResp(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg.getDirection() == DirectionConstant.RESPONSE) {
            CommandFuture messageFuture = futureMap.remove(msg.getId());
            if (messageFuture != null) {
                boolean isSuccess = msg.getCode() != ProtocalConstant.REMOTE_ERROR;
                CompletableFuture<RemotingCommand> future = messageFuture.getRespFuture();
                if (isSuccess) {
                    future.complete(msg);
                } else {
                    RemoteExceptionBody remoteExceptionBody = RemotingCommand.doDecode(msg.getBody(), RemoteExceptionBody.class);
                    future.completeExceptionally(new RemoteException(remoteExceptionBody));
                }
            }
        }
    }

    public RemotingCommand buildError(RemotingCommand message, Throwable throwable) {
        return buildResponse(message, new RemoteExceptionBody(throwable), ProtocalConstant.REMOTE_ERROR);
    }

    protected abstract class AbstractHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            doProcess(ctx, msg);
        }
    }
}
