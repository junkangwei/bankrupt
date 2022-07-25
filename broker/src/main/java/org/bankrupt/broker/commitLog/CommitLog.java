package org.bankrupt.broker.commitLog;

import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.broker.delay.DelayMessageService;
import org.bankrupt.broker.store.DispatchRequest;
import org.bankrupt.broker.store.MappedFile;
import org.bankrupt.broker.store.MappedFileQueue;
import org.bankrupt.broker.store.SelectMappedBufferResult;
import org.bankrupt.common.constant.HeaderConstant;
import org.bankrupt.common.util.MapUtil;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * commitLog配置类
 */
public class CommitLog {

    private static Logger log = Logger.getLogger(CommitLog.class);

    public static final String MAGICCODE = "WJK";

    private final DefaultMessageStore defaultMessageStore;

    protected final MappedFileQueue mappedFileQueue;

    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    private final static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock writeLock = lock.writeLock();
    private Lock readLock = lock.readLock();
    //我们commitLog的格式
    // 4 TOTALSIZE +
    // 8 整体的偏移量 PHYSIZE +
    // 8 BORNTIME(创建时间) +
    // 4 TOPIC的名字 +
    // 4 RESUMESIZE(重试次数) +
    // 4 QUEUEID +
    // 8 QUEUEOFFSET +
    // 4 BODYSIZE +
    // BODYSIZE BDOY内容
    // 4 HEADERSIZE
    // HEADERSIZE HEADER的内容
    public static int BASIC_LENGTH =4 + 8 + 8 + 4 + 4 + 4 + 8 + 4;
    public static int TOTAL_SIZE = 4;

    public CommitLog(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog()
                ,defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public boolean load() {
        //初始化
        boolean result = this.mappedFileQueue.load();
        System.out.println("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * 插入数据
     * @param message
     */
    public void putMessage(Message message) {
        MappedFile mappedFile = this.getMappedFileQueue().getLastMappedFile(0L);
        //加锁 RW锁
        Long queueOffset = message.getQueueOffset();
        writeLock.lock();
        try {
            mappedFile.appendMessage(message);
            String key = message.getTopic() + "-" + message.getQueueId();
            this.topicQueueTable.put(key, ++queueOffset);
        }catch (Exception e){
            log.error("异常了:{}",e);
        }finally {
            writeLock.unlock();
        }
    }

    /**
     * 获得当前最大的offset
     * @return
     */
    public long getMaxoffset() {
        CopyOnWriteArrayList<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if(mappedFiles.size() == 0){
            return 0;
        }
        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);
        return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
    }

    /**
     * 开始获取消息
     * @param reputFromOffset
     */
    public SelectMappedBufferResult getMessage(long reputFromOffset) {
        //查询消息在哪
        int commitLogSize = defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        CopyOnWriteArrayList<MappedFile> mappedFiles = defaultMessageStore.getCommitLog().getMappedFileQueue().getMappedFiles();
        //二分获得在那个mappedFile,之后在实现
        for (int i = 0; i < mappedFiles.size(); i++) {

        }
        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);
        //在这个文件上的偏移量
        int pos = (int) (reputFromOffset % commitLogSize);
        return mappedFile.selectMappedBuffer(pos);
    }

    /**
     * 获得消息，然后返回大小
     * @param byteBuffer
     */
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer) {
        try {
            // 4 TOTALSIZE +
            int size = byteBuffer.getInt();
            if(size == 0){
                return new DispatchRequest(-1,false);
            }
            // 8 整体的偏移量 PHYSIZE +
            long physize = byteBuffer.getLong();
            // 8 BORNTIME(创建时间) +
            long bornTime = byteBuffer.getLong();
            // 4 TOPIC的名字 +
            byte[] topic = new byte[4];
            byteBuffer.get(topic);
            String topicName = new String(topic);
            log.info("topic的名字是:" + topicName);
            // 4 RESUMESIZE(重试次数) +
            int time = byteBuffer.getInt();
            // 4 QUEUEID +
            int queueId = byteBuffer.getInt();
            // 8 CONSUMEQUEUEOFFSET +
            Long queueOffset = byteBuffer.getLong();
            // 4 BODYSIZE +
            int bodySize = byteBuffer.getInt();
            // body的长度
            byte[] body = new byte[bodySize];
            byteBuffer.get(body);
            log.info("body里面的内容是是:" + new String(body));
            // 4 HEADERSIZE +
            int headerSize = byteBuffer.getInt();
            byte[] header = new byte[headerSize];
            byteBuffer.get(header);
            String headers = new String(header, StandardCharsets.UTF_8);
            log.info("header里面的内容是是:" + new String(header));
            Map<String,String> map =(Map<String,String>) MapUtil.getValue(headers);
            if(map.containsKey(HeaderConstant.LEVEL)){
                Integer level = Integer.valueOf(map.get(HeaderConstant.LEVEL));
                Long future = DelayMessageService.getDelayLevelTimeMap().get(level);
                bornTime += future;
            }
            return new DispatchRequest(topicName,queueId,physize,size,queueOffset,bornTime);
        }catch (Exception e){

        }
        return new DispatchRequest(-1,false);
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public Message getMessage(long cmOffset, int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.getMappedFileQueue().getLastMappedFile(cmOffset);
        if(mappedFile != null){
            int pos = (int) (cmOffset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos, size);
            if(result == null){
                return null;
            }
            ByteBuffer byteBuffer = result.getByteBuffer();
            return getMessage(byteBuffer);
        }
        return null;
    }


    public Message getMessage(ByteBuffer byteBuffer) {
        Message message = new Message();
        // 4 TOTALSIZE +
        int size = byteBuffer.getInt();
        // 8 整体的偏移量 PHYSIZE +
        long physize = byteBuffer.getLong();
        // 8 BORNTIME(创建时间) +
        long bornTime = byteBuffer.getLong();
        message.setBornTime(bornTime);
        // 4 TOPIC的名字 +
        byte[] topic = new byte[4];
        byteBuffer.get(topic);
        String topicName = new String(topic);
        message.setTopic(topicName);
        log.info("topic的名字是:" + topicName);
        // 4 RESUMESIZE(重试次数) +
        int time = byteBuffer.getInt();
        // 4 QUEUEID +
        int queueId = byteBuffer.getInt();
        message.setQueueId(queueId);
        // 8 CONSUMEQUEUEOFFSET +
        Long queueOffset = byteBuffer.getLong();
        message.setQueueOffset(queueOffset);
        // 4 BODYSIZE +
        int bodySize = byteBuffer.getInt();
        // body的长度
        byte[] body = new byte[bodySize];
        byteBuffer.get(body);
        message.setBody(body);
        log.info("body里面的内容是是:" + new String(body));
        // 4 HEADERSIZE +
        int headerSize = byteBuffer.getInt();
        byte[] header = new byte[headerSize];
        byteBuffer.get(header);
        log.info("header里面的内容是是:" + new String(header));
        message.setHeader(new String(header));
        return message;
    }

    public long loadData() {
        //恢复文件
        CopyOnWriteArrayList<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        log.info("开始恢复commitLog的文件了");
        long max = 0;
        if (!mappedFiles.isEmpty()) {
            //获得buffer
            MappedFile mappedFile = mappedFiles.get(0);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            int fileSize = mappedFile.getFileSize();
            long mappedFileOffset = 0;
            for (; ; ) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer);
                if (dispatchRequest != null) {
                    //如果是成功的
                    int size = dispatchRequest.getMsgSize();
                    if (dispatchRequest.getSuccess() && size > 0) {
                        mappedFileOffset += size;
                    } else {
                        log.info("恢复到没有消息了");
                        break;
                    }
                } else {
                    log.info("恢复commitLog的消息为空");
                    break;
                }
            }
            mappedFileOffset += processOffset;
            mappedFile.setWrotePosition((int) (mappedFileOffset % fileSize));
            max = mappedFileOffset;
        }
        return max;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void flush() {
        for (MappedFile mappedFile : this.mappedFileQueue.getMappedFiles()) {
            //判断一下
            if(mappedFile != null){
                //判断刷盘点和提交点
                if(mappedFile.needFlush()){
                    mappedFile.flush();
                }
            }
        }
    }
}
