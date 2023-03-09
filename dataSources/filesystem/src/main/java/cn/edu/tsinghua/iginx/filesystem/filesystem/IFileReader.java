package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface IFileReader {
    // read the file by bytes
    ByteFileReader();
    // read the file by lines
    TextFileReader();
}
