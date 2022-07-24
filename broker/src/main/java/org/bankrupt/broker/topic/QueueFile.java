package org.bankrupt.broker.topic;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * topic的队列
 */
public class QueueFile {
    //队列号，文件id
    private Integer id;

    private RandomAccessFile randomAccessFile;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;
}
