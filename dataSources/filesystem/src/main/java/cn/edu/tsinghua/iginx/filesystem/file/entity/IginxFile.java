package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.filesystem.file.Operator;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class IginxFile implements Operator {
    @Override
    public List<Record> read(File file) throws IOException {
        return null;
    }

    @Override
    public Exception write(File file, List<Record> values) {
        return null;
    }
}
