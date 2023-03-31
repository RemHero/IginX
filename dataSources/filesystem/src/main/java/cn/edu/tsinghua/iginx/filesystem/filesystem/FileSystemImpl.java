package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileType;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.utils.TimeUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/*
 *缓存，索引以及优化策略都在这里执行
 */
public class FileSystemImpl {
    IFileOperator fileOperator;
    Charset charset = StandardCharsets.UTF_8;

    // set the fileSystem type with constructor
    public FileSystemImpl(/*FileSystemType type*/) {
        fileOperator = new DefaultFileOperator();
        FilePath.setSeparator(System.getProperty("file.separator"));
    }

    public List<Record> readFile(File file) throws IOException {
        return readFile(file, -1, -1);
    }

    // read the part of the file
    public List<Record> readFile(File file, long begin, long end) throws IOException {
        Path path = Paths.get(file.getPath());

        if (!Files.exists(path))
            return new ArrayList<>();

        return doReadFile(file, begin, end);
    }

    // not support the begin and end, fix it
    private List<Record> doReadFile(File file, long begin, long end) throws IOException {
        List<Record> res = new ArrayList<>();
        List<byte[]> valList = new ArrayList<>();
        long key = TimeUtils.MIN_AVAILABLE_TIME;
        switch (FileType.getFileType(file)) {
            case IGINX_FILE:
                res = fileOperator.IginxFileReaderByKey(file, begin, end, charset);
                break;
            case TEXT_FILE:
                valList = fileOperator.TextFileReader(file, charset);
                break;
            default:
                valList = fileOperator.TextFileReader(file, charset);
        }
        for (byte[] val : valList) {
            res.add(new Record(key++, val));
        }
        return res;
    }

    // write single file with bytes
    public Exception writeFile(File file, List<Record> value, boolean append) throws IOException {
        return doWriteFile(file, value, append);
    }

    // write multi file
    public Exception writeFiles(List<File> files, List<List<Record>> values, List<Boolean> appends) throws IOException {
        for (int i = 0; i < files.size(); i++) {
            writeFile(files.get(i), values.get(i), appends.get(i));
        }
        return null;
    }

    public Exception deleteFile(File file) {
        return deleteFiles(Collections.singletonList(file));
    }

    /**
     * 删除文件或目录
     *
     * @param files 要删除的文件或目录列表
     * @throws Exception 如果删除操作失败则抛出异常
     */
    public Exception deleteFiles(List<File> files) {
        for (File file : files) {
            Stack<File> stack = new Stack<>();
            stack.push(file);
            while (!stack.isEmpty()) {
                File currentFile = stack.pop();
                if (currentFile.isDirectory()) {
                    File[] fileList = currentFile.listFiles();
                    if (fileList != null && fileList.length > 0) {
                        for (File subFile : fileList) {
                            stack.push(subFile);
                        }
                    }
                }
                if (!currentFile.delete()) {
                    return new IOException("Failed to delete file: " + currentFile.getAbsolutePath());
                }
            }
        }
        return null;
    }

    private Exception doWriteFile(File file, List<Record> value, boolean append) throws IOException {
        Exception res;

        byte[] bytes;
        switch (FileType.getFileType(file)) {
            case IGINX_FILE:
                res = fileOperator.IginxFileWriter(file, value);
                break;
            case TEXT_FILE:
                bytes = makeValueToBytes(value);
                res = fileOperator.TextFileWriter(file, bytes, append);
                break;
            default:
                bytes = makeValueToBytes(value);
                res = fileOperator.TextFileWriter(file, bytes, append);
        }
        return res;
    }

    private byte[] makeValueToBytes(List<Record> value) throws IOException {
        List<byte[]> byteList = new ArrayList<>();
        switch (value.get(0).getDataType()) {
            case BINARY:
            default:
                for (Record val : value) {
                    byteList.add((byte[]) val.getRawData());
                }
                break;
        }
        return mergeByteArrays(byteList);
    }

    public byte[] mergeByteArrays(List<byte[]> arrays) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            outputStream.write(array);
        }
        return outputStream.toByteArray();
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }
}
