import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.broker.commitLog.CommitLog;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
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
 * 消息测试
 */
public class TestMessage {
    public static Logger LOGGER = Logger.getLogger(TestMessage.class);
    DefaultMessageStore defaultMessageStore;

    private String filePath;
    private MappedByteBuffer mappedByteBuffer;
    @Before
    public void before() {
        defaultMessageStore = new DefaultMessageStore();
        defaultMessageStore.start();
    }

    //开始写文件
    @Test
    public void putMessage() throws InterruptedException {
        final CommitLog commitLog = defaultMessageStore.getCommitLog();
        //获得最后一个文件，然后开始写入消息
        Message message = new Message();
        message.setBody("wjk".getBytes(StandardCharsets.UTF_8));
        message.setTopic("test");
        message.setBornTime(System.currentTimeMillis());
        message.setQueueId(0);
        message.setQueueOffset(0L);
        final HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("queueId","0");
        stringStringHashMap.put("topic","test");
        message.setHeader(stringStringHashMap.toString());
        commitLog.putMessage(message);
        Thread.sleep(1000000000);
    }

    public static void main(String[] args) throws IOException {
        final TestMessage testMessage = new TestMessage();
        testMessage.testConsumeQueue();
    }

    public void testConsumeQueue() throws IOException {
        filePath = "D:\\Bankrupt\\store\\consumeQueue\\test\\0\\00000000000000000000";
        init(0, 5 * 1024 * 1024);
        readConsumeQueue(1);
        readConsumeQueue(2);
        readConsumeQueue(3);
    }

    public void testReadCommitLog() throws IOException {
        filePath = "D:\\Bankrupt\\store\\commitlog\\00000000000000000000";
        init(0, 5 * 1024 * 1024);
        readCommitLog(1);
        readCommitLog(2);
        readCommitLog(3);
        readCommitLog(4);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
//        readCommitLog(3);
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
        LOGGER.info("size" + size);
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
