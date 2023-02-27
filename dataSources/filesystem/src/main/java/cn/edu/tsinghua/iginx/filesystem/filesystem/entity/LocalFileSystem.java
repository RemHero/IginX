package cn.edu.tsinghua.iginx.filesystem.filesystem.entity;

import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import static cn.edu.tsinghua.iginx.filesystem.FileSystem.MAXFILESIZE;

public class LocalFileSystem implements FileSystemOperator {
    private static final Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);
    @Override
    public byte[] readFileByBytes(String fileName) {
        byte[] res = new byte[MAXFILESIZE];
        File file = new File(fileName);
        InputStream in;
        int currentSize = 0;
        try {
            // 一次读一个字节
            in = Files.newInputStream(file.toPath());
            int tempbyte;
            while ((tempbyte = in.read()) != -1) {
                res[currentSize++] = (byte) tempbyte;
                if(currentSize >= MAXFILESIZE)
                    logger.error("the file size is lager than MAXFILESIZE {}", MAXFILESIZE);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public int writeFileWithBytes(String fileName, byte[] val) {
        return 0;
    }
}
