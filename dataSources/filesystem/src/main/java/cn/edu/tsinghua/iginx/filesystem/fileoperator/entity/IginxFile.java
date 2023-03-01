package cn.edu.tsinghua.iginx.filesystem.fileoperator.entity;

import cn.edu.tsinghua.iginx.filesystem.fileoperator.FileOperator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.util.List;

public class IginxFile implements FileOperator {
    // read the parquet file
    @Override
    public List<Record> queryFiles(File file) {
        return null;
    }

    @Override
    public Exception writeFileData(File file, List<Record> values) {
        return null;
    }
}
