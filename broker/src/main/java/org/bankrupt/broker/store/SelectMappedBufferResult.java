package org.bankrupt.broker.store;

import java.nio.ByteBuffer;

public class SelectMappedBufferResult {

    private final long startOffset;

    private final ByteBuffer byteBuffer;

    private int size;

    private MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public void setMappedFile(MappedFile mappedFile) {
        this.mappedFile = mappedFile;
    }

}
