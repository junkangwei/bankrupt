package org.bankrupt.remoting.common;

/**
 * 生命周期
 */
public interface RemoteLifeCycle {
    /**
     * 启动
     */
    void start();

    /**
     * 关闭
     */
    void close();
}
