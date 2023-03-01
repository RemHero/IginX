package cn.edu.tsinghua.iginx.filesystem.fileoperator.entity;

import cn.edu.tsinghua.iginx.filesystem.fileoperator.FileOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;

import static cn.edu.tsinghua.iginx.filesystem.FileSystem.MAXFILESIZE;

public class NormalFile implements FileOperator {
    @Override
    public List<Record> queryFiles(File file) {
        InputStream in;
        int currentSize = 0;
        File file = new File(fileName);
        // 一次读一个字节 may fix it
        in = Files.newInputStream(file.toPath());
        int tempbyte;
        while ((tempbyte = in.read()) != -1) {
            res.add((byte) tempbyte);
            if(currentSize >= MAXFILESIZE)
                logger.error("the file size is lager than MAXFILESIZE {}", MAXFILESIZE);
        }
        in.close();
    }

    @Override
    public Exception writeFileData(File file, List<Record> values) {
        return null;
    }
}
