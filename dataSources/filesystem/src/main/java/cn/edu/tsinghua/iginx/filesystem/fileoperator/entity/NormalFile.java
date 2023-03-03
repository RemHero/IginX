package cn.edu.tsinghua.iginx.filesystem.fileoperator.entity;

import cn.edu.tsinghua.iginx.filesystem.fileoperator.FileOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


public class NormalFile implements FileOperator {

    public byte[] readFileToByteArrayUsingStream(Path path) throws IOException {
        try (InputStream inputStream = Files.newInputStream(path);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
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

    @Override
    public Exception writeFileData(File file, List<Record> values) {
        return null;
    }

    @Override
    public List<Record> queryFiles(File file) throws IOException {
        List<Record> res = new ArrayList<>();
        byte[] byteArray = readFileToByteArrayUsingStream(file.toPath());
        for (byte val : byteArray) {
        }
    }
}
