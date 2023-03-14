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

    Exception ByteFileWriter(File file, byte[] bytes values, boolean append);

    // read the file by lines
    Exception TextFileWriter(File file, byte[] bytes values, boolean append);
}
