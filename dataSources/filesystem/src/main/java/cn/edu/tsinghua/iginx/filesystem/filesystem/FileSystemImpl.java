package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileSystemImpl {
    public List<Record> readFile(File file) throws IOException;
    // read the part of the file
    public List<Record> readFile(File file, long begin, long end) throws IOException;
    // write single file with bytes
    public Exception writeFile(File file, List<Record> values);
    // write multi file
    public Exception writeFiles(List<File> file, List<List<Record>> values);
}
