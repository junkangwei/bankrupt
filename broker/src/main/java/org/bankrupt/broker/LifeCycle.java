package org.bankrupt.broker;

public interface LifeCycle {
    //父类实现
    default void start() {
        // 啥都不做
    }

    default void close() {
        // 啥都不做
    }
}
