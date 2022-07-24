package org.bankrupt.remoting.common.exception;

public class RemoteException extends RuntimeException {

    public RemoteException(Throwable e) {
        super(e);
    }

    public RemoteException(RemoteExceptionBody remoteExceptionBody) {
        super(remoteExceptionBody.getMessage() + "\n" + remoteExceptionBody.getStackTraceElementsMessage());
    }

}
