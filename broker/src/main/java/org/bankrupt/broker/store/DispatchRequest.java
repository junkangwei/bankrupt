/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bankrupt.broker.store;

import java.util.Map;

public class DispatchRequest {
    private final String topic;
    private final int queueId;
    private final long commitLogOffset;
    private final long queueOffset;
    private final long storeTime;
    private int msgSize;
    private Boolean success;

    public DispatchRequest(String topic, int queueId, long commitLogOffset, int msgSize,long queueOffset,long storeTime) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.queueOffset = queueOffset;
        this.storeTime = storeTime;
        this.success = true;
    }

    public DispatchRequest(int msgSize,boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = msgSize;
        this.queueOffset = 0;
        this.storeTime = 0;
        this.success = success;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public long getQueueOffset() {
        return queueOffset;
    }


    public long getStoreTime() {
        return storeTime;
    }
}
