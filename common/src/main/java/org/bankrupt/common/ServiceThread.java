package org.bankrupt.common;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ServiceThread implements Runnable {

    private Thread thread;
    protected volatile boolean stopped = false;
    protected boolean isDaemon = false;
    private final AtomicBoolean started = new AtomicBoolean(false);


    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
    }

    public abstract String getServiceName();

    public boolean isStopped() {
        return stopped;
    }
}
