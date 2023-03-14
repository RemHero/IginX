package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface IFileOperator {
    // read the file by bytes
    List<Record> ByteFileReader(File file);

    // read the file by lines
    List<Record> TextFileReader(File file);

    // read the line of [begin,end] of text file
    List<Record> TextFileReader(File file, long begin, long end) throws IOException;

    List<Record> ByteFileWriter(File file, List<Record> values);

    // read the file by lines
    List<Record> TextFileWriter(File file, List<Record> values);
}
