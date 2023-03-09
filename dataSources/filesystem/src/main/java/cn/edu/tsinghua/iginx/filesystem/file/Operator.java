package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;

/*
 * 对于不同类型数据，以不同的方式读/写
 */
public interface Operator {
    List<Record> getRecord(List<Object> rawData);
}
