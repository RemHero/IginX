package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileType;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/*
 *缓存，索引以及优化策略都在这里执行
 */
public class FileSystemImpl {
    IFileOperator fileOperator;
    Charset charset;

    // set the fileSystem type with constructor
    public FileSystemImpl(/*FileSystemType type*/) {
        fileOperator = new DefaultFileOperator();
    }

    public List<Record> readFile(File file) throws IOException {
        return readFile(file, -1, -1);
    }

    // read the part of the file
    public List<Record> readFile(File file, long begin, long end) throws IOException {
        if (begin == -1 && end == -1) {
            
        }
        return doReadFile(file, begin, end);
    }

    public List<Record> doReadFile(File file, long begin, long end) throws IOException {
        List<Record> res;
        switch (FileType.getFileType(file)) {
            case IGINX_FILE:
            case TEXT_FILE:
                res = fileOperator.TextFileReader(file);
                break;
            default:
                res = fileOperator.TextFileReader(file);
        }
        return res;
    }

    // write single file with bytes
    public Exception writeFile(File file, List<Record> value, boolean append) throws IOException {
        return doWriteFile(file, value, append);
    }

    // write multi file
    public Exception writeFiles(List<File> file, List<List<Record>> values, boolean append) {
        return null;
    }

    public Exception doWriteFile(File file, List<Record> value, boolean append) throws IOException {
        Exception res;
        switch (FileType.getFileType(file)) {
            case IGINX_FILE:
            case TEXT_FILE:
                res = fileOperator.TextFileWriter(file, value, append);
                break;
            default:
                res = fileOperator.TextFileWriter(file, value, append);
        }
        return res;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }
}
