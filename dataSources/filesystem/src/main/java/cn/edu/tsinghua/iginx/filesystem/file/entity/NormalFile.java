package cn.edu.tsinghua.iginx.filesystem.file.entity;


import cn.edu.tsinghua.iginx.filesystem.file.Operator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.utils.TimeUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class NormalFile implements Operator {
    private int BUFFERSIZE = 1024;
    public byte[] readFileToByteArrayUsingStream(Path path) throws IOException {
        try (InputStream inputStream = Files.newInputStream(path);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[BUFFERSIZE];
            int numRead = 0;
            long fileSize = Files.size(path);
            byte[] byteArray = new byte[(int) fileSize];

            while ((numRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, numRead);
            }

            byteArray = outputStream.toByteArray();
            return byteArray;
        }
    }
    public NormalFile() {

    }

    @Override
    public List<Record> read(File file) throws IOException {
        List<Record> res = new ArrayList<>();
        long timestamp = TimeUtils.MIN_AVAILABLE_TIME;
        byte[] byteArray = readFileToByteArrayUsingStream(file.toPath());
        for (byte val : byteArray) {
            res.add(new Record(timestamp++, val));
        }
        return res;
    }

    @Override
    public Exception write(File file, List<Record> values) {
        return null;
    }


//
//    @Override
//    public Exception write(File file, List<Record> values) {
//        return null;
//    }
//
//    @Override
//    public List<Record> read(File file) throws IOException {
//        List<Record> res = new ArrayList<>();
//        long timestamp = TimeUtils.MIN_AVAILABLE_TIME;
//        byte[] byteArray = readFileToByteArrayUsingStream(file.toPath());
//        for (byte val : byteArray) {
//            res.add(new Record(timestamp++, val));
//        }
//        return res;
//    }
}
