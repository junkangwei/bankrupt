package org.bankrupt.broker.topic;

import org.apache.log4j.Logger;
import org.bankrupt.broker.commitLog.CommitLog;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.store.DispatchRequest;
import org.bankrupt.broker.store.MappedFile;
import org.bankrupt.broker.store.MappedFileQueue;
import org.bankrupt.broker.store.SelectMappedBufferResult;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConsumeQueue {
    private static Logger log = Logger.getLogger(ConsumeQueue.class);

    //8位的偏移量 + 4位的size
    public static final int CQ_STORE_UNIT_SIZE = 8 + 4 + 8;

    private DefaultMessageStore defaultMessageStore;


    private final MappedFileQueue mappedFileQueue;
    //topic名称
    private final String topic;
    //0 1 2 3
    private final int queueId;
    //存储的路径
    private final String storePath;

    private final int mappedFileSize;

    private long maxPhysicOffset = -1;

    private volatile long minLogicOffset = 0;

    private final ByteBuffer byteBuffer;

    public ConsumeQueue(String topic,
                        int queueId,
                        String storePath,
                        int mappedFileSize,
                        DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.topic = topic;
        this.queueId = queueId;
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;
        //D:\rocketmq-master\store\consumequeue\TopicTest\0 600W
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize);
        this.mappedFileQueue.load();

        this.byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    }

    public void dispatch(DispatchRequest dispatchRequest) {
        this.byteBuffer.flip();
        this.byteBuffer.limit(CQ_STORE_UNIT_SIZE);
        this.byteBuffer.putLong(dispatchRequest.getCommitLogOffset());
        this.byteBuffer.putInt(dispatchRequest.getMsgSize());
        this.byteBuffer.putLong(dispatchRequest.getStoreTime());

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
            mappedFile.appendMessageByte(this.byteBuffer.array());
        }
    }

    public long getMinOffset() {
        return minLogicOffset;
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public SelectMappedBufferResult getMoreMessage(long offset) {
        int mappedFileSize = this.mappedFileSize;
        long nowOffset = (long) offset * CQ_STORE_UNIT_SIZE;
        //如果进入了说明有新的消息
        if (offset >= this.getMinLogicOffset()) {
            //找到mappedFile通过偏移量
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                //获得具体的bytebuffer
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) nowOffset);
                return result;
            }
        }
        return null;
    }

    public int recover() {
        //开始恢复
        CopyOnWriteArrayList<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        int count = 0;
        if (!mappedFiles.isEmpty()) {
            int cqSize = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(0);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            for (int i = 0; i < cqSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();//8 commitLog offset
                int size = byteBuffer.getInt();//4位的大小
                long storeTime = byteBuffer.getLong();//8位的消费时间
                if (offset >= 0 && size > 0) {//859851
                    count++;
                    mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                    this.maxPhysicOffset = offset + size;
                } else {
                    log.info("当前queue的名字是：" + mappedFile.getFileName() + "没有数据了");
                    break;
                }
            }
            processOffset += mappedFileOffset;
            mappedFile.setWrotePosition((int) processOffset % cqSize);
            mappedFile.setFlushPosition((int) processOffset % cqSize);
        }
        return count;
    }
}
