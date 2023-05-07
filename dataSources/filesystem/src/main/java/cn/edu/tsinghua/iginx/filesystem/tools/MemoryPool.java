package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemoryPool {
    private static final Logger logger = LoggerFactory.getLogger(MemoryPool.class);
    private static int blockSize= 1024; // 1024 bytes per block
    private static ConcurrentLinkedQueue<byte[]> freeBlocks;
    private static int poolSize;

    public static void initMemoryPool(int sizeInGigabytes, int blockLen){
        blockSize=blockLen;
        poolSize = sizeInGigabytes;
        int numberOfBlocks = poolSize / blockSize;
        freeBlocks = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < numberOfBlocks; i++) {
            freeBlocks.add(new byte[blockSize]);
        }
    }

    public static byte[] allocate(int size) {
        byte[] buffer = freeBlocks.poll();
        if (buffer == null) {
            logger.warn("Out of memory: No more blocks available");
            return new byte[size];
        }
        return buffer;
    }

    public static void release(byte[] buffer) {
        freeBlocks.offer(buffer);
    }
}
