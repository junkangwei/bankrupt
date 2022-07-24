package org.bankrupt.broker.config;

import java.io.File;

public class BrokerConfig {
    private int brokerId = 0;
    private int port = 9999;
    private String host = "127.0.0.1";
    private int queueNumber = 8;
    private String storagePath = System.getProperty("user.home") + File.separator + "Bankrupt";

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int delayOffsetInterval = 1000 * 5;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public void setQueueNumber(int queueNumber) {
        this.queueNumber = queueNumber;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }

    public int getDelayOffsetInterval() {
        return delayOffsetInterval;
    }

    public void setDelayOffsetInterval(int delayOffsetInterval) {
        this.delayOffsetInterval = delayOffsetInterval;
    }
}
