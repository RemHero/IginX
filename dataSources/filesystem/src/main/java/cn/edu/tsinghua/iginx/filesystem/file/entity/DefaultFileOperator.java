package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DefaultFileOperator implements IFileOperator {
    private static final Logger logger = LoggerFactory.getLogger(DefaultFileOperator.class);
    private int BUFFERSIZE = 1024;

    @Override
    public List<Record> ByteFileReader(File file) {
        return null;
    }

    @Override
    public List<Record> TextFileReader(File file) {
        return null;
    }

    @Override
    public List<Record> TextFileReader(File file, long begin, long end) throws IOException {
        List<Record> res = new ArrayList<>();
        long timestamp = TimeUtils.MIN_AVAILABLE_TIME;
        byte[] byteArray = readFileToByteArrayUsingStream(file.toPath());
        for (byte val : byteArray) {
            res.add(new Record(timestamp++, val));
        }
        return res;
    }

    @Override
    public Exception ByteFileWriter(File file, byte[] bytes, boolean append) {
        return null;
    }

    @Override
    public Exception TextFileWriter(File file, byte[] bytes, boolean append) {
        return null;
    }

    public void writeToFile(String content, File file, boolean append) throws IOException {
        // 将字符串转换为字节数组
        byte[] bytes = content.getBytes(charset);

        // 使用Java NIO将字节数组写入文件
        Path path = file.toPath();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // 创建OpenOption选项数组
        StandardOpenOption[] options;
        if (append) {
            options = new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND};
        } else {
            options = new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        }

        // 使用OpenOption选项数组写入文件
        Files.write(path, buffer.array(), options);
    }

    public byte[] readFileToByteArrayUsingStream(Path path) throws IOException {
        try (InputStream inputStream = Files.newInputStream(path);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[BUFFERSIZE];
            int numRead = 0;
            byte[] byteArray;

            while ((numRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, numRead);
            }

            byteArray = outputStream.toByteArray();
            return byteArray;
        }
    }
}
