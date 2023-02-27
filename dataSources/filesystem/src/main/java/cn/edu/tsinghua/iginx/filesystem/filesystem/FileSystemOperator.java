package cn.edu.tsinghua.iginx.filesystem.filesystem;

import java.util.List;

public interface FileSystemOperator {
    // read the file by bytes
    public byte[] readFileByBytes(String fileName);

    // write the file woth bytes
    public int writeFileWithBytes(String fileName, byte[] val);
}
