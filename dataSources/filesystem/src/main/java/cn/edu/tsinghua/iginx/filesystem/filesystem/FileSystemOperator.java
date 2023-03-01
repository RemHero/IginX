package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.util.List;

public interface FileSystemOperator {
    // read the file by bytes
    public List<Record> read(File file);

    // write the file woth bytes
    public Exception write(File file, List<Record> values);
}
