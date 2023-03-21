package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

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

        long currentLine = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                currentLine++;
                if (currentLine < FileMeta.IGINXFILEVALINDEX) continue;
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

    private String convertObjectToString(Object obj, DataType type) {
        if (obj == null) {
            return null;
        }

        if (type == null) {
            type = BINARY;
        }

        String strValue = null;
        try {
            switch (type) {
                case BINARY:
                    strValue = new String((byte[]) obj);
                    break;
                case INTEGER:
                    strValue = Integer.toString((int) obj);
                    break;
                case DOUBLE:
                    strValue = Double.toString((double) obj);
                    break;
                case FLOAT:
                    strValue = Float.toString((float) obj);
                    break;
                case BOOLEAN:
                    strValue = Boolean.toString((boolean) obj);
                    break;
                case LONG:
                    strValue = Long.toString((long) obj);
                    break;
                default:
                    strValue = null;
                    break;
            }
        } catch (Exception e) {
            strValue = null;
        }

        return strValue;
    }

    private final String recordToString(Record record) {
        return record.getKey() + "," + convertObjectToString(record.getRawData(), record.getDataType());
    }

    private Exception appendValToIginxFile(File file, List<Record> valList) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            for (Record record : valList) {
                writer.write(recordToString(record));
            }
        }
        return null;
    }

    private void createIginxFile(File file, Record record) throws IOException {
        Path csvPath = Paths.get(file.getPath());

        try {
            if (!Files.exists(csvPath)) {
                Files.createFile(csvPath);
            } else {
                return;
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvPath.toFile()))) {
                writer.newLine();
                writer.write(record.getDataType().toString());
                for (int i = 0; i < FileMeta.IGINXFILEVALINDEX - 2; i++) {
                    writer.newLine();
                }
            }

        } catch (IOException e) {
            throw new IOException("Cannot create file: " + file.getAbsolutePath());
        }
    }

    int getFileLineNumber(File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            int lines = 0;
            while (reader.readLine() != null) {
                lines++;
            }
            return lines;
        } catch (IOException e) {
            throw new IOException("Cannot get file: " + file.getAbsolutePath());
        }
    }

    private void replaceFile(File file, File tempFile) throws IOException {
        if (!tempFile.exists()) {
            throw new IOException("Temp file does not exist.");
        }
        if (!file.exists()) {
            throw new IOException("Original file does not exist.");
        }
        Files.move(tempFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public Exception IginxFileWriter(File file, List<Record> valList) throws IOException {
        int BUFFER_SIZE = 8192; // 8 KB
        if (file.exists() && file.isDirectory()) {
            throw new IOException("Cannot write to directory: " + file.getAbsolutePath());
        }

        if (!file.exists()) {
            createIginxFile(file, valList.get(0));
            return appendValToIginxFile(file, valList);
        }

        if (getFileLineNumber(file) == FileMeta.IGINXFILEVALINDEX) {
            return appendValToIginxFile(file, valList);
        }

        // Check if valList can be directly appended to the end of the file
        if (file.exists() && file.length() > 0) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                long lastKey = 0L;
                raf.seek(Math.max(file.length() - BUFFER_SIZE, 0));
                byte[] buffer = new byte[BUFFER_SIZE];
                int n = raf.read(buffer);
                String lastLine = new String();
                while (n != -1) {
                    String data = new String(buffer, 0, n);
                    String[] lines, fields;
                    lastLine += data;
                    if (!lastLine.contains("\n") || lastLine.indexOf("\n") == lastLine.lastIndexOf("\n")) {//是否确切包含了最后一行
                        raf.seek(Math.max(raf.getFilePointer() - BUFFER_SIZE, 0));
                        n = raf.read(buffer);
                        continue;
                    }
                    lines = lastLine.split("\\r?\\n");
                    fields = lines[lines.length - 1].split(",", 2);
                    lastKey = Long.parseLong(fields[0]);
                    if (lastKey < valList.get(0).getKey()) {
                        return appendValToIginxFile(file, valList);
                    } else {
                        break;
                    }
                }
            }
        }

        // Create temporary file
        File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
        BufferedWriter writer = null;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            writer = new BufferedWriter(new FileWriter(tempFile));
            int valIndex = 0, maxLen = valList.size();
            long minKey = Math.min(valList.get(0).getKey(), Long.MAX_VALUE);
            long currentLine = 0L;
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    currentLine++;
                    if (currentLine < FileMeta.IGINXFILEVALINDEX) {
                        writer.write(line);
                        writer.newLine();
                        continue;
                    }
                    String[] kv = line.split(",", 2);
                    long key = Long.parseLong(kv[0]);
                    boolean isCovered = false;
                    while (key >= minKey) {
                        if (key == minKey) isCovered = true;
                        if (valIndex >= maxLen) break;
                        Record record = valList.get(valIndex++);
                        minKey = record.getKey();
                        writer.write(recordToString(record));
                    }
                    if (!isCovered) {
                        writer.write(line);
                    }
                    if (valIndex >= maxLen) break;
                }
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        replaceFile(file, tempFile);
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
