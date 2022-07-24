package org.bankrupt.remoting.core.config;

/**
 * netty基础配置
 */
public class RemoteConfig {
    /**
     * 主机地址
     */
    private String host;
    /**
     * 监听的端口号
     */
    private int port;
    /**
     * boss group 的线程数 默认为1
     */
    private int bossThreads;
    /**
     * work group的线程数 work默认为
     */
    private int workThreads;

    private int timeOut = 5000000;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBossThreads() {
        return bossThreads;
    }

    public void setBossThreads(int bossThreads) {
        this.bossThreads = bossThreads;
    }

    public int getWorkThreads() {
        return workThreads;
    }

    public void setWorkThreads(int workThreads) {
        this.workThreads = workThreads;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }
}
