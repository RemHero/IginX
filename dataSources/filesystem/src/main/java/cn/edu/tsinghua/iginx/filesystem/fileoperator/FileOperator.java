package cn.edu.tsinghua.iginx.filesystem.fileoperator;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.util.List;

/*
 * 对于不同类型数据，以不同的方式读/写
 */
public interface FileOperator {
    List<Record> queryFiles(File file);

    Exception writeFileData(File file, List<Record> values);
}
