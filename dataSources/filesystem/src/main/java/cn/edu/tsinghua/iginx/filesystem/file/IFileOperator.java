package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public interface IFileOperator {
    // read the file by bytes
//    List<byte[]> ByteFileReader(File file);

    // read the all the file
    List<byte[]> TextFileReader(File file, Charset charset) throws IOException;

    // read the file by lines [begin, end]
    List<byte[]> TextFileReaderByLine(File file, long begin, long end, Charset charset) throws IOException;

    // read the all the file
//    List<byte[]> IginxFileReader(File file);

    // read the file by lines [begin, end]
//    List<byte[]> IginxFileReaderByKey(File file, long begin, long end) throws IOException;

    // read the byte range [begin, end] of the file
//    List<Record> TextFileReaderByByteSeek(File file, long begin, long end) throws IOException;

//    Exception ByteFileWriter(File file, byte[] bytes, boolean append);

    Exception TextFileWriter(File file, byte[] bytes, boolean append) throws IOException;
}
