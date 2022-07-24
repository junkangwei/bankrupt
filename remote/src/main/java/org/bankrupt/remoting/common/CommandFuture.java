package org.bankrupt.remoting.common;


import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 返回值封装
 */
public class CommandFuture {
    /**
     * 请求的内容
     */
    private RemotingCommand remotingCommand;
    /**
     * 返回的内容 未来式, 也可能不返回
     */
    private CompletableFuture<RemotingCommand> respFuture;
    /**
     * 创建时间
     */
    private long createTime;
    /**
     * 超时时间
     */
    private long timeoutTime;


    public CommandFuture(RemotingCommand message, long timeOut, TimeUnit timeUnit) {
        this.remotingCommand = message;
        this.respFuture = new CompletableFuture<>();
        this.createTime = System.currentTimeMillis();
        this.timeoutTime = timeUnit.toMillis(timeOut);
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public void setRemotingCommand(RemotingCommand remotingCommand) {
        this.remotingCommand = remotingCommand;
    }

    public CompletableFuture<RemotingCommand> getRespFuture() {
        return respFuture;
    }

    public void setRespFuture(CompletableFuture<RemotingCommand> respFuture) {
        this.respFuture = respFuture;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getTimeoutTime() {
        return timeoutTime;
    }

    public void setTimeoutTime(long timeoutTime) {
        this.timeoutTime = timeoutTime;
    }

    public boolean isTimeOut() {
        return System.currentTimeMillis() - createTime > timeoutTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandFuture that = (CommandFuture) o;
        return createTime == that.createTime && timeoutTime == that.timeoutTime && Objects.equals(remotingCommand, that.remotingCommand) && Objects.equals(respFuture, that.respFuture);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remotingCommand, respFuture, createTime, timeoutTime);
    }
}
