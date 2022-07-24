package com.bankrupt.client.config;

/**
 * 客户端配置类
 */
public class ClientConfig {
    /**
     * broker的地址
     */
    private String brokerHost = "127.0.0.1";
    private int brokerPort = 9999;


    /**
     * 没有消息下一次拉取消息的间隔
     */
    private int pullMessageInterval = 30000;
    /**
     * 每次拉取消息的最大数量
     */
    private int pullMessageNumber = 20;

    private int sendThreadNum = 32;

    public int getSendThreadNum() {
        return sendThreadNum;
    }

    public void setSendThreadNum(int sendThreadNum) {
        this.sendThreadNum = sendThreadNum;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public void setBrokerHost(String brokerHost) {
        this.brokerHost = brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
    }

    public int getPullMessageInterval() {
        return pullMessageInterval;
    }

    public void setPullMessageInterval(int pullMessageInterval) {
        this.pullMessageInterval = pullMessageInterval;
    }

    public int getPullMessageNumber() {
        return pullMessageNumber;
    }

    public void setPullMessageNumber(int pullMessageNumber) {
        this.pullMessageNumber = pullMessageNumber;
    }
}
