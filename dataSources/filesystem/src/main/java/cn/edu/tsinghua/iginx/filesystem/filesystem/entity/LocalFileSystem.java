package cn.edu.tsinghua.iginx.filesystem.filesystem.entity;

import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class LocalFileSystem implements FileSystemOperator {
    private static final Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);

    @Override
    public List<Record> read(File file) {
        return null;
    }

    @Override
    public Exception write(File file, List<Record> values) {
        return null;
    }
}
