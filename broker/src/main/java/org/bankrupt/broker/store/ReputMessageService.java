package org.bankrupt.broker.store;

import org.apache.log4j.Logger;
import org.bankrupt.broker.commitLog.CommitLog;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.common.ServiceThread;

import java.util.concurrent.atomic.AtomicBoolean;


public class ReputMessageService extends ServiceThread{
    public static Logger LOGGER = Logger.getLogger(ReputMessageService.class);

    private volatile long reputFromOffset = 0;

    private DefaultMessageStore defaultMessageStore;

    private Thread thread;

    private final AtomicBoolean started = new AtomicBoolean(false);

    protected volatile boolean stopped = false;

    public long getReputFromOffset() {
        return reputFromOffset;
    }

    public void setReputFromOffset(long reputFromOffset) {
        this.reputFromOffset = reputFromOffset;
    }

    public ReputMessageService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public void consumeOffset() {
        //如果有，说明是有消息的
        CommitLog commitLog = this.defaultMessageStore.getCommitLog();
        for(boolean next = true ; this.hasNewOffset() && next;){
            SelectMappedBufferResult result = commitLog.getMessage(reputFromOffset);
            if(result != null){
                try {
                    this.reputFromOffset = result.getStartOffset();
                    for(int size = 0;size < result.getSize();){
                        DispatchRequest dispatchRequest = defaultMessageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer());
                        int msgSize = dispatchRequest.getMsgSize();
                        if(dispatchRequest.getSuccess()){
                            if(msgSize > 0){
                                defaultMessageStore.doDispatch(dispatchRequest);
                                this.reputFromOffset += msgSize;
                                size += msgSize;
                            }else {
                                size = result.getSize();
                                continue;
                            }
                        }else{
                            break;
                        }
                    }
                }finally {

                }
            }else {
                //没有消息了
                next = false;
            }
        }
    }

    private boolean hasNewOffset() {
        return reputFromOffset < defaultMessageStore.getCommitLog().getMaxoffset();
    }

    @Override
    public void run() {
        while (!this.isStopped()){
            try {
                Thread.sleep(10000);
                this.consumeOffset();
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.error("异常的原因是:" + e.getMessage());
            }
        }
        LOGGER.info("ReputMessageService退出了");
    }

    @Override
    public String getServiceName() {
        return ReputMessageService.class.getSimpleName();
    }
}
