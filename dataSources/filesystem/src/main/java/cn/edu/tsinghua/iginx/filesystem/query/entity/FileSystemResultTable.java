package cn.edu.tsinghua.iginx.filesystem.query.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.util.List;
import java.util.Map;

public class FileSystemResultTable {

  private File file;

  private List<Record> records;

  private DataType dataType;

  private Map<String, String> tags;

  public FileSystemResultTable(
      File file, List<Record> records, DataType dataType, Map<String, String> tags) {
    this.file = file;
    this.records = records;
    this.dataType = dataType;
    this.tags = tags;
  }

  public List<Record> getRecords() {
    return records;
  }

  public void setRecords(List<Record> records) {
    this.records = records;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public String toString() {
    StringBuilder res = new StringBuilder();
    int i=10;
    for (Record record : records) {
      res.append(record.toString());
      res.append("\n");
      i--;
      if (i<0) break;
    }
    return "FileSystemResultTable{" +
        "file=" + file +
        ", records=" + res.toString() +
        ", dataType=" + dataType +
        ", tags=" + tags +
        '}';
  }
}
