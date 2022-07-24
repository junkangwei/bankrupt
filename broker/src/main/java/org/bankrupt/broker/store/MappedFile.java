package org.bankrupt.broker.store;

import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.broker.commitLog.CommitLog;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MappedFile {

    public static Logger log = Logger.getLogger(MappedFile.class);

    private DefaultMessageStore messageStore;

    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    protected int fileSize;
    private String fileName;
    private long fileFromOffset;
    private File file;

    //mmap
    protected FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    /**
     * unmap force刷盘使用
     */
    private static final Method MMAP_CLEAR_METHOD;
    static {
        try {
            MMAP_CLEAR_METHOD = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            MMAP_CLEAR_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public MappedFile(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());//开头的偏移量
        boolean ok = false;

        parentExist(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel(); //获得fileChannel
            byte[] bytes = new byte[this.fileSize];
            fileChannel.read(ByteBuffer.wrap(bytes));
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);//总的偏移量
            ok = true;
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public static void parentExist(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }


    //判断文件是否已经写满
    public boolean isFull() {
        return fileSize == this.wrotePosition.get();
    }

    /**
     * 写入消息
     * @param message
     */
    public void appendMessage(Message message) {
        appendMessage(message,false);
    }

    /**
     * 写入消息
     * @param message 消息猪蹄
     * @param flush 是否刷盘 true刷盘，false不刷盘
     */
    public void appendMessage(Message message, Boolean flush){
        Integer writePosition = this.wrotePosition.get();
        ByteBuffer slice = this.mappedByteBuffer.slice();
        slice.position(writePosition);
        ByteBuffer buffer = encodeMessage(message,this.fileFromOffset + writePosition);
        int size =buffer.limit();
        if(writePosition + size > this.fileSize){
            //说明文件太大了 创建一个文件来存储
        }
        this.wrotePosition.set(writePosition + size);
        slice.put(buffer);
        if(flush){
            mappedByteBuffer.force();
        }
    }

    /**
     * encode message
     * @param message
     * @param phyOffset
     * @return
     */
    private ByteBuffer encodeMessage(Message message, Long phyOffset) {
        // 消息长度4 + 40 +5 +4 + 23
        byte[] body = message.getBody();//消息长度5
        byte[] header = message.getHeader().getBytes(StandardCharsets.UTF_8);//23
        int messageLength = CommitLog.BASIC_LENGTH + body.length + 4 + header.length;//72
        ByteBuffer buffer = ByteBuffer.allocate(messageLength);
        buffer.putInt(messageLength);
        buffer.putLong(phyOffset);
        buffer.putLong(message.getBornTime());
        String topic = message.getTopic();
        buffer.put(topic.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(message.getConsumeTime());
        buffer.putInt(message.getQueueId());
        buffer.putLong(message.getQueueOffset());
        buffer.putInt(body.length);
        buffer.put(body);
        buffer.putInt(header.length);
        buffer.put(header);

        buffer.position(0);
        return buffer;
    }

    /**
     * 获取消息
     * @param pos
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(pos);
        int size = this.wrotePosition.get() - pos;
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);
        return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
    }

    public void appendMessageByte(byte[] array) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + array.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(array));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(array.length);
        }
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = this.wrotePosition.get();
        if ((pos + size) <= readPosition){
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            ByteBuffer byteBufferNew = byteBuffer.slice();
            byteBufferNew.limit(size);
            return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
        }
        return null;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public int getFileSize() {
        return fileSize;
    }

    public void flush() {
        if(this.mappedByteBuffer != null){
            this.mappedByteBuffer.force();
        }
    }
}
