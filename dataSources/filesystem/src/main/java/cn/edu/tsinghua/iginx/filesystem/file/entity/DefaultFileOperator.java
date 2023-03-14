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
    public List<Record> ByteFileWriter(File file, List<Record> values) {
        return null;
    }

    @Override
    public List<Record> TextFileWriter(File file, List<Record> values) {
        return null;
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
