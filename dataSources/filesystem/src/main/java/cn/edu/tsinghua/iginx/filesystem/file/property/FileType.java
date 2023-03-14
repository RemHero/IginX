package cn.edu.tsinghua.iginx.filesystem.file.property;

import java.io.File;

public class FileType {

    public static enum Type {
        IGINX_FILE,
        NORMAL_FILE
    }

    public static Type getFileType(File file) {
        String fileName = file.getName();
        if (fileName.endsWith(".iginx")) {
            return Type.IGINX_FILE;
        } else if (fileName.endsWith(".txt")) {
            return Type.NORMAL_FILE;
        } else {
            return Type.NORMAL_FILE;
        }
    }
}
