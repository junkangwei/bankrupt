package org.bankrupt.broker.store;

import org.apache.log4j.Logger;
import org.bankrupt.broker.topic.ConsumeQueue;
import org.bankrupt.common.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;

public class MappedFileQueue {

    private static Logger log = Logger.getLogger(MappedFileQueue.class);

    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    public MappedFileQueue(String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    public CopyOnWriteArrayList<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    /**
     * 20位
     * commitLog初始化
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    mappedFile.setWrotePosition(mappedFile.getFileSize());
                    this.mappedFiles.add(mappedFile);
                } catch (IOException e) {
                    return false;
                }
            }
        }


        return true;
    }

    /**
     * 创建文件 我这边是同步创建，rocketmq可以同步也可以异步创建
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(Long startOffset){
        //这边到时候要考虑并发
        long createOffset = -1;
        MappedFile mappedFile = null;
        if(mappedFiles.size() > 0){
            mappedFile = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }
        //说明需要创建commitlog
        if(mappedFile == null){
            mappedFile = createMappedFile(0L);
            if(mappedFile != null){
                mappedFiles.add(mappedFile);
            }
        }else{
            //如果文件不为空，判断文件是否满了
            if(mappedFile.isFull()){
                //如果满了创建一个文件
                createOffset = mappedFile.getFileFromOffset() + this.mappedFileSize;
                mappedFile = createMappedFile(createOffset);
                if(mappedFile != null){
                    mappedFiles.add(mappedFile);
                }
            }
        }
        return mappedFile;
    }

    private MappedFile createMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + FileUtil.offset2FileName(createOffset);
        MappedFile mappedFile = null;
        try {
            mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
        } catch (IOException e) {
            System.out.println("创建文件失败");
            e.printStackTrace();
        }
        return mappedFile;
    }

    public static void main(String[] args) {
        String path = System.getProperty("user.dir") + File.separator + "store" + File.separator + "commitLog";
        MappedFileQueue mappedFileQueue = new MappedFileQueue(path, 5 * 1024);
        final boolean load = mappedFileQueue.load();
        for (MappedFile mappedFile : mappedFileQueue.getMappedFiles()) {
            System.out.println("文件名是:" + mappedFile.getFileName());
        }
//        MappedFile lastMappedFile = mappedFileQueue.getLastMappedFile(0L);
//        mappedFileQueue.getLastMappedFile(0L);
//        mappedFileQueue.getLastMappedFile(0L);
//        System.out.println(lastMappedFile);
    }

    public MappedFile getLastMappedFile() {
        //这边到时候要考虑并发
        long createOffset = -1;
        MappedFile mappedFile = null;
        if(mappedFiles.size() > 0){
            mappedFile = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }
        //说明需要创建commitlog
        if(mappedFile == null){
            mappedFile = createMappedFile(0L);
            if(mappedFile != null){
                mappedFiles.add(mappedFile);
            }
        }else{
            //如果文件不为空，判断文件是否满了
            if(mappedFile.isFull()){
                //如果满了创建一个文件
                createOffset = mappedFile.getFileFromOffset() + this.mappedFileSize;
                mappedFile = createMappedFile(createOffset);
                if(mappedFile != null){
                    mappedFiles.add(mappedFile);
                }
            }
        }
        return mappedFile;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);
        if(mappedFile != null){
            return mappedFile.getWrotePosition() / ConsumeQueue.CQ_STORE_UNIT_SIZE;
        }
        return 0;
    }
}
