package org.bankrupt.broker.store;

import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.topic.ConsumeQueue;

/**
 * commitlog处理
 */
public class ConsumeQueueDispatcher implements CommitLogDispatcher{

    private DefaultMessageStore defaultMessageStore;

    public ConsumeQueueDispatcher(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public void doDispatch(DispatchRequest dispatchRequest) {
        ConsumeQueue consumeQueue = defaultMessageStore.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        consumeQueue.dispatch(dispatchRequest);
    }
}
