package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface IFileOperator {
    // read the file by bytes
    List<Record> ByteFileReader(File file);

    // read the all the file
    List<Record> TextFileReader(File file);

    // read the file by lines [begin, end]
    List<Record> TextFileReaderByLine(File file, long begin, long end) throws IOException;

    // read the all the file
    List<Record> IginxFileReader(File file);

    // read the file by lines [begin, end]
    List<Record> IginxFileReaderByKey(File file, long begin, long end) throws IOException;

    // read the byte range [begin, end] of the file
//    List<Record> TextFileReaderByByteSeek(File file, long begin, long end) throws IOException;

    Exception ByteFileWriter(File file, byte[] bytes, boolean append);

    Exception TextFileWriter(File file, byte[] bytes, boolean append);
}
