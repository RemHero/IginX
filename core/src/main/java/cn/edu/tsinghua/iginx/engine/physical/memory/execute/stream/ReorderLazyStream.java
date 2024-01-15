package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReorderLazyStream extends UnaryLazyStream {

  private final Reorder reorder;

  private Header header;

  private Map<Integer, Integer> reorderMap;

  private Row nextRow = null;
  private long execTime = 0;

  public ReorderLazyStream(Reorder reorder, RowStream stream) {
    super(stream);
    this.reorder = reorder;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (this.header == null) {
      Header header = stream.getHeader();
      List<Field> targetFields = new ArrayList<>();
      this.reorderMap = new HashMap<>();

      for (int index = 0; index < reorder.getPatterns().size(); index++) {
        String pattern = reorder.getPatterns().get(index);
        List<Pair<Field, Integer>> matchedFields = new ArrayList<>();
        if (StringUtils.isPattern(pattern)) {
          for (int i = 0; i < header.getFields().size(); i++) {
            Field field = header.getField(i);
            if (StringUtils.match(field.getName(), pattern)) {
              matchedFields.add(new Pair<>(field, i));
            }
          }
        } else {
          for (int i = 0; i < header.getFields().size(); i++) {
            Field field = header.getField(i);
            if (pattern.equals(field.getName())) {
              matchedFields.add(new Pair<>(field, i));
            }
          }
        }
        if (!matchedFields.isEmpty()) {
          // 不对同一个UDF里返回的多列进行重新排序
          if (!reorder.getIsPyUDF().get(index)) {
            matchedFields.sort(Comparator.comparing(pair -> pair.getK().getFullName()));
          }
          matchedFields.forEach(
              pair -> {
                reorderMap.put(targetFields.size(), pair.getV());
                targetFields.add(pair.getK());
              });
        }
      }
      this.header = new Header(header.getKey(), targetFields);
    }
    return this.header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  private Row calculateNext() throws PhysicalException {
    Header header = getHeader();
    List<Field> targetFields = header.getFields();
    if (stream.hasNext()) {
      Row row = stream.next();
      Object[] values = new Object[targetFields.size()];
      for (int i = 0; i < values.length; i++) {
        values[i] = row.getValue(reorderMap.get(i));
      }
      if (header.hasKey()) {
        return new Row(header, row.getKey(), values);
      } else {
        return new Row(header, values);
      }
    }
    return null;
  }

  @Override
  public Row next() throws PhysicalException {
    long startTime = System.nanoTime();
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row row = nextRow;
    nextRow = null;
    long endTime = System.nanoTime();
    execTime += (endTime - startTime);
    return row;
  }

  @Override
  public Batch nextBatch() throws PhysicalException {
    return stream.nextBatch();
  }

  @Override
  public boolean hasNextBatch() throws PhysicalException {
    return stream.hasNextBatch();
  }

  @Override
  public String printExecTime() throws PhysicalException {
    System.out.println(stream.printExecTime());
    return "Reorder Exec Time: " + execTime;
  }
}
