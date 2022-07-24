package org.bankrupt.broker.commitLog;

import org.bankrupt.broker.topic.ConsumeQueue;

import java.io.File;

public class MessageStoreConfig {
    //commitLog的位置
    private static final String STORE_HOME = "/Users/weijunkang/Bankrupt/store";

    private String storePathCommitLog = STORE_HOME
            + File.separator + "commitlog";
    //consumeQueue的位置
    private String storePathConsumeQueue = STORE_HOME
            + File.separator + "consumequeue";

    //commitlog的大小  1G
//    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;
    //测试使用
    private int mappedFileSizeCommitLog = 5 * 1024 * 1024;

    private int mappedFileSizeConsumeQueue = 300000;

    private int flushCommitLog = 1000 * 5;

    public int getFlushCommitLog() {
        return flushCommitLog;
    }

    public void setFlushCommitLog(int flushCommitLog) {
        this.flushCommitLog = flushCommitLog;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public String getStorePathConsumeQueue() {
        return storePathConsumeQueue;
    }

    public static void main(String[] args) {
        System.out.println(System.getProperty("user.home") + File.separator + "store"
                + File.separator + "commitlog");

        System.out.println(System.getProperty("user.home") + File.separator + "store"
                + File.separator + "consumequeue");
    }

    public int getMappedFileSizeConsumeQueue() {
        return mappedFileSizeConsumeQueue;
    }
}
