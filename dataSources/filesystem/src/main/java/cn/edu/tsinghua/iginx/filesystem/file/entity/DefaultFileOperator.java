package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.*;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private Map<String, String> readIginxMetaInfo(File file) throws IOException {
        Map<String, String> result = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        int lineCount = 1;
        while ((line = reader.readLine()) != null) {
            if (lineCount == 1) {
                result.put("series", line);
            } else if (lineCount == 2) {
                result.put("type", line);
            }
            lineCount++;
        }
        reader.close();
        return result;
    }

    public List<Record> readIginxFileByKey(File file, long begin, long end, Charset charset) throws IOException {
        Map<String, String> fileInfo = readIginxMetaInfo(file);
        List<Record> res = new ArrayList<>();
        long key = TimeUtils.MIN_AVAILABLE_TIME;
        if (begin == -1 && end == -1) {
            begin = 0;
            end = Long.MAX_VALUE;
        }
        if (begin < 0 || end < 0 || (begin > end)) {
            throw new IOException("Read information outside the boundary with BEGIN " + begin + " and END " + end);
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] kv = line.split(",", 2);
                key = Long.parseLong(kv[0]);
                if (key >= begin && key <= end) {
                    res.add(new Record(Long.parseLong(kv[0]), DataTypeUtils.strToDataType(fileInfo.get("type")), kv[1]));
                }
            }
        }
        return res;
    }

//    private static class MyReadSupport extends ReadSupport<Object[]> {
//        @Override
//        public ReadContext init(InitContext context) {
//            MessageType schema = context.getFileMetaData().getSchema();
//            List<BlockMetaData> blocks = context.getBlocks();
//            List<Type> types = schema.getFields();
//            return new ReadContext(schema, types.toArray(new Type[0]), blocks);
//        }
//
//        @Override
//        public Object[] read(Binary value, ReadContext context) {
//            Object[] record = new Object[2];
//            record[0] = value;
//            record[1] = context.getData().read();
//            return record;
//        }
//    }


    @Override
    public List<Record> IginxFileReaderByKey(File file, long begin, long end, Charset charset) throws IOException {
        return readIginxFileByKey(file, begin, end, charset);
    }

    @Override
    public Exception IginxFileWriter(File file, List<Record> valList) throws IOException {
        
        return null;
    }

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
