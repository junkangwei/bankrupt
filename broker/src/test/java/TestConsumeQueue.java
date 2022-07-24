import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.broker.commitLog.CommitLog;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.store.ReputMessageService;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * 消息写入同步到comsumequeue
 */
public class TestConsumeQueue {
    public static Logger LOGGER = Logger.getLogger(TestConsumeQueue.class);
    DefaultMessageStore defaultMessageStore;

    private String filePath;
    private MappedByteBuffer mappedByteBuffer;
    @Before
    public void before() {
        defaultMessageStore = new DefaultMessageStore();
        defaultMessageStore.start();
    }


    public static void main(String[] args) throws IOException {
        final TestConsumeQueue testMessage = new TestConsumeQueue();
        testMessage.testReadCommitLog();
    }

    public void testConsumeQueue() throws IOException {
        filePath = "D:\\Bankrupt\\store\\consumeQueue\\test\\0\\00000000000000000000";
        init(0, 5 * 1024 * 1024);
        readConsumeQueue(1);
        readConsumeQueue(2);
    }

    public void testReadCommitLog() throws IOException {
        filePath = "D:\\Bankrupt\\store\\commitlog\\00000000000000000000";
        init(0, 5 * 1024 * 1024);
        readCommitLog(1);
        readCommitLog(2);
        readCommitLog(3);
        readCommitLog(4);
        readCommitLog(5);
        readCommitLog(6);
        readCommitLog(7);
        readCommitLog(8);
        readCommitLog(9);
    }


    private void init(int position, long size) throws IOException {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(new File(filePath), "rw");
        FileChannel channel = (FileChannel) randomAccessFile.getChannel();
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        mappedByteBuffer.position(position);
    }

    private void readConsumeQueue(int i) {
        LOGGER.info("当前进来的index是:" + i);
        Long phySize = mappedByteBuffer.getLong();
        LOGGER.debug("Message phySize:[{}]" + phySize);
        int size = mappedByteBuffer.getInt();
        LOGGER.info("size:" + size);
        Long storeTime = mappedByteBuffer.getLong();
        LOGGER.info("storeTime:" + storeTime);
    }

    private void readCommitLog(int i) {
        LOGGER.info("当前进来的index是:" + i);
        int size = mappedByteBuffer.getInt();
        LOGGER.info("size" + size);
        Long phySize = mappedByteBuffer.getLong();
        LOGGER.debug("Message phySize:[{}]" + phySize);
        Long BornTimestamp = mappedByteBuffer.getLong();
        LOGGER.debug("Message BornTimestamp:[{}]" + BornTimestamp);
        byte[] topic = new byte[4];
        mappedByteBuffer.get(topic);
        LOGGER.debug("Message topic:[{}]" + new String(topic));
        int time = mappedByteBuffer.getInt();
        LOGGER.debug("Message ConsumeQueue time:[{}]"+ time);
        int queueId = mappedByteBuffer.getInt();
        LOGGER.debug("Message CommitLog queueId:[{}]" + queueId);
        Long queueOffset = mappedByteBuffer.getLong();
        LOGGER.debug("Message CommitLog queueOffset:[{}]" + queueOffset);
        int bodyLength = mappedByteBuffer.getInt();
        LOGGER.debug("Message BodyLength:[{}]" + bodyLength);
        byte[] body = new byte[bodyLength];
        mappedByteBuffer.get(body);
        LOGGER.debug("Message Body:[{}]" + new String(body));

        int headerLength = mappedByteBuffer.getInt();
        LOGGER.debug("Message headerLength:[{}]" + headerLength);
        byte[] header = new byte[headerLength];
        mappedByteBuffer.get(header);
        LOGGER.debug("Message header:[{}]" + new String(header));
    }

}
