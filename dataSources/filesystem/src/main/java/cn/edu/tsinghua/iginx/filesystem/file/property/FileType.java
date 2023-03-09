package cn.edu.tsinghua.iginx.filesystem.file.property;

import cn.edu.tsinghua.iginx.filesystem.file.Operator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.IginxFile;
import cn.edu.tsinghua.iginx.filesystem.file.entity.NormalFile;

import java.io.File;

public class FileType {

    public static enum Type {
        INGINX_FILE,
        NORMAL_FILE
    }

    public static Type getFileType(File file) {
        String fileName = file.getName();
        if (fileName.endsWith(".iginx")) {
            return Type.INGINX_FILE;
        } else if (fileName.endsWith(".txt")) {
            return Type.NORMAL_FILE;
        } else {
            return Type.NORMAL_FILE;
        }
    }

    public static Operator getOpertatorWithFileType(Type type) {
        switch (type) {
            case INGINX_FILE:
                return new IginxFile();
            case NORMAL_FILE:
                return new NormalFile();
            default:
                return new NormalFile();
        }
    }
}
