package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class DefaultFileOperator implements IFileOperator {
    private static final Logger logger = LoggerFactory.getLogger(DefaultFileOperator.class);
    private int BUFFERSIZE = 1024;

//    @Override
//    public List<Record> ByteFileReader(File file, Charset charset) {
//        return null;
//    }

    @Override
    public List<byte[]> TextFileReader(File file, Charset charset) throws IOException {
        return TextFileReaderByLine(file, -1, -1, charset);
    }

    @Override
    public List<byte[]> TextFileReaderByLine(File file, long begin, long end, Charset charset) throws IOException {
        List<byte[]> res = new ArrayList<>();
        long key = TimeUtils.MIN_AVAILABLE_TIME;
        if (begin == -1 && end == -1) {
            begin = 0;
            end = Long.MAX_VALUE;
        }
        if (begin < 0 || end < 0) {
            throw new IOException("Read information outside the boundary with BEGIN " + begin + " and END " + end);
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            long currentLine = 0;
            String line;
            while ((line = reader.readLine()) != null && currentLine <= end) {
                if (currentLine >= begin) {
                    res.add(line.getBytes(charset));
                }
                currentLine++;
            }
        }
        return res;
    }

    // may fix it when use the cache and seek-tree
//    @Override
//    public List<Record> TextFileReaderByByteSeek(File file, long begin, long end) throws IOException {
//        RandomAccessFile raf = new RandomAccessFile(file, "r");
//        long position = begin;  // 指定从文件的第50个字节位置开始读取
//        raf.seek(position);
//        FileChannel channel = raf.getChannel();
//        ByteBuffer buffer = ByteBuffer.allocate(1024);
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(raf.getFD())));
//        String line;
//        while ((line = reader.readLine()) != null) {
//            System.out.println(line);
//        }
//        raf.close();
//    }


//    @Override
//    public List<Record> IginxFileReader(File file) {
//        return null;
//    }

//    @Override
//    public List<Record> IginxFileReaderByKey(File file, long begin, long end) throws IOException {
//        return null;
//    }

//    @Override
//    public Exception ByteFileWriter(File file, byte[] bytes, boolean append) {
//        return null;
//    }

    @Override
    public Exception TextFileWriter(File file, byte[] bytes, boolean append) throws IOException {
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
        return null;
    }

    public void writeToFile(byte[] bytes, File file, boolean append) throws IOException {

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
