package org.bankrupt.broker.store;

public interface CommitLogDispatcher {
    void doDispatch(DispatchRequest dispatchRequest);
}
