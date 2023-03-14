package cn.edu.tsinghua.iginx.filesystem.filesystem.entity;

import cn.edu.tsinghua.iginx.filesystem.file.property.FileType;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.List;

public class DefaultFileReader implements IFileReader {
    private static final Logger logger = LoggerFactory.getLogger(DefaultFileReader.class);

    @Override
    public List<Record> readFile(File file) throws IOException {
        FileSystem fs  = FileSystems.getDefault();
        java.nio.file.FileSystem localFileSystem = FileSystems.getDefault();
        localFileSystem.
        Operator operator = FileType.getOpertatorWithFileType(FileType.getFileType(file));
        return null;
    }

    @Override
    public Exception writeFile(File file, List<Record> values) {
        return null;
    }

    public class
}
