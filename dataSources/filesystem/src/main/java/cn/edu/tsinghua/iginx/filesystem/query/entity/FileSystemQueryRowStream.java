package cn.edu.tsinghua.iginx.filesystem.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.tools.FilePathUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSystemQueryRowStream implements RowStream {

  private final Header header;

  private final List<FileSystemResultTable> rowData;

  private final int[] indices;

  private int hasMoreRecords = 0;

  private Filter filter;

  private Row nextRow = null;

  //tmp
  private boolean isDouble = false;

  private int pos = 0;
  private int batchSize = Batch.BATCH_SIZE;
  private double[] doubles  = new double[batchSize];
  private int[] ints = new int[batchSize];
  private long[] keys = new long[batchSize];
  private byte[] bitmap = new byte[batchSize];
  private long execTime = 0;

  public FileSystemQueryRowStream(
      List<FileSystemResultTable> result, String storageUnit, String root, Filter filter) {
    Field time = Field.KEY;
    List<Field> fields = new ArrayList<>();

    this.filter = filter;
    this.rowData = result;

    String column;
    for (FileSystemResultTable resultTable : rowData) {
      File file = resultTable.getFile();
      column = FilePathUtils.convertAbsolutePathToPath(root, file.getAbsolutePath(), storageUnit);
      Field field = new Field(column, resultTable.getDataType(), resultTable.getTags());
      fields.add(field);
    }

    this.indices = new int[rowData.size()];
    this.header = new Header(time, fields);
    for (FileSystemResultTable row : rowData) {
      if (!row.getRecords().isEmpty()) {
        hasMoreRecords++;
      }
    }
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    // need to do nothing
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null && hasMoreRecords != 0) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    long startTime = System.nanoTime();
    if (!hasNext()) {
      throw new PhysicalException("no more data");
    }
    Row currRow = nextRow;
    nextRow = calculateNext();
    long endTime = System.nanoTime();
    execTime += (endTime - startTime);
    return currRow;
  }

  private Row getNext() throws PhysicalException {
    long key = Long.MAX_VALUE;
    for (int i = 0; i < rowData.size(); i++) {
      int index = indices[i];
      List<Record> records = rowData.get(i).getRecords();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      Record record = records.get(index);
      key = Math.min(record.getKey(), key);
    }
    if (key == Long.MAX_VALUE) {
      return null;
    }
    Object[] values = new Object[rowData.size()];
    for (int i = 0; i < rowData.size(); i++) {
      int index = indices[i];
      List<Record> records = rowData.get(i).getRecords();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      Record record = records.get(index);
      if (record.getKey() == key) {
        Object value = record.getRawData();
        values[i] = value;
        indices[i]++;
        if (indices[i] == records.size()) {
          hasMoreRecords--;
        }
      }
    }
    return new Row(header, key, values);
  }

  private Row calculateNext() throws PhysicalException {
    Row row = getNext();
    while (row != null) {
      if (filter == null || FilterUtils.validate(filter, row)) {
        return row;
      }
      row = getNext();
    }
    return null;
  }

  @Override
  public Batch nextBatch() throws PhysicalException {
    long startTime = System.nanoTime();
    for (FileSystemResultTable resultTable : rowData) {
      List<Record> data = resultTable.getRecords();
      if (data.isEmpty()) continue;
      if (resultTable.getDataType()== DataType.DOUBLE && !data.isEmpty()) {
        isDouble = true;
        for (int i=pos;i<pos+batchSize;i++)  {
          doubles[i-pos] = (Double)data.get(i).getRawData();
          keys[i-pos] = data.get(i).getKey();
          bitmap[i-pos] = 1;
        }
      } else {
        for (int i=pos;i<pos+batchSize;i++)  {
          ints[i-pos] = (Integer) data.get(i).getRawData();
          keys[i-pos] = data.get(i).getKey();
          bitmap[i-pos] = 1;
        }
      }
      pos += batchSize;
    }

    Batch batch = new Batch(header, keys, isDouble ? doubles : ints);
    batch.setBitmap(bitmap);
    long endTime = System.nanoTime();
    execTime += (endTime - startTime);
    return batch;
  }

  @Override
  public boolean hasNextBatch() throws PhysicalException {
    return pos<10485760;
  }

  @Override
  public String printExecTime() throws PhysicalException {
    return "Table Scan Exec Time: " + execTime;
  }
}
