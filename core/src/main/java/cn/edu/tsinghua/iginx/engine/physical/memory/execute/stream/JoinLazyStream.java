/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.thrift.DataType;
import jdk.incubator.vector.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinLazyStream extends BinaryLazyStream {

  private final Join join;

  private boolean joinByTime = false;

  private boolean joinByOrdinal = false;

  private boolean hasInitialized = false;

  private boolean hasIntersect = false;

  private Map<Field, Integer> fieldIndices;

  private Header header;

  private Row nextA;

  private Row nextB;

  //vec imp
  private Batch batchA;
  private Batch batchB;
  private Batch batchResult = new Batch();
  final VectorSpecies<Double> SPECIES_DOUBLE = DoubleVector.SPECIES_PREFERRED;
  final VectorSpecies<Integer> SPECIES_INTEGER = IntVector.SPECIES_PREFERRED;
  final VectorSpecies<Long> SPECIES_LONG = LongVector.SPECIES_PREFERRED;
  private double[] doubleResult = new double[Batch.BATCH_SIZE];
  private int[] intResult = new int[Batch.BATCH_SIZE];
  private long[] keyResult = new long[Batch.BATCH_SIZE];
  private long execTime = 0;

  public JoinLazyStream(Join join, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.join = join;
  }

  private void initialize() throws PhysicalException {
    if (hasInitialized) {
      return;
    }
    if (this.join.getJoinBy().equals(Constants.KEY)) {
      joinByTime = true;
    }
    if (this.join.getJoinBy().equals(Constants.ORDINAL)) {
      joinByOrdinal = true;
    }
    if (!joinByTime && !joinByOrdinal) {
      throw new InvalidOperatorParameterException(
          "join operator is not support for field "
              + join.getJoinBy()
              + " except for "
              + Constants.KEY
              + " and "
              + Constants.ORDINAL);
    }
    Header headerA = streamA.getHeader();
    Header headerB = streamB.getHeader();
    for (Field field : headerA.getFields()) {
      Field relatedField = headerB.getFieldByName(field.getFullName());
      if (relatedField != null) {
        if (relatedField.getType() != field.getType()) {
          throw new PhysicalException(
              "path "
                  + field.getFullName()
                  + " has two different types: "
                  + field.getType()
                  + ", "
                  + relatedField.getType());
        }
        hasIntersect = true;
      }
    }
    List<Field> newFields = new ArrayList<>();
    fieldIndices = new HashMap<>();
    for (Field field : headerA.getFields()) {
      if (fieldIndices.containsKey(field)) {
        continue;
      }
      fieldIndices.put(field, newFields.size());
      newFields.add(field);
    }
    for (Field field : headerB.getFields()) {
      if (fieldIndices.containsKey(field)) {
        continue;
      }
      fieldIndices.put(field, newFields.size());
      newFields.add(field);
    }

    if (joinByTime) {
      if (!headerA.hasKey() || !headerB.hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by time should have timestamp.");
      }
      header = new Header(Field.KEY, newFields);
    } else {
      if (headerA.hasKey() || headerB.hasKey()) {
        throw new InvalidOperatorParameterException(
            "row streams for join operator by ordinal shouldn't have timestamp.");
      }
      header = new Header(newFields);
    }
    hasInitialized = true;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    return nextA != null && nextB != null || streamA.hasNext() || streamB.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    long startTime = System.nanoTime();
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    if (nextA == null && streamA.hasNext()) {
      nextA = streamA.next();
    }
    if (nextB == null && streamB.hasNext()) {
      nextB = streamB.next();
    }
    if (nextA == null) { // 流 A 被消费完毕
      Row row = nextB;
      nextB = null;
      // 做一个转换
      long endTime = System.nanoTime();
      execTime += (endTime - startTime);
      return buildRow(null, row);
    }
    if (nextB == null) { // 流 B 被消费完毕
      Row row = nextA;
      nextA = null;
      long endTime = System.nanoTime();
      execTime += (endTime - startTime);
      return buildRow(row, null);
    }
    if (joinByOrdinal) {
      Row row = buildRow(nextA, nextB);
      nextA = null;
      nextB = null;
      long endTime = System.nanoTime();
      execTime += (endTime - startTime);
      return row;
    }
    if (joinByTime) {
      Row row;
      if (nextA.getKey() == nextB.getKey()) {
        row = buildRow(nextA, nextB);
        nextA = null;
        nextB = null;
      } else if (nextA.getKey() < nextB.getKey()) {
        row = buildRow(nextA, null);
        nextA = null;
      } else {
        row = buildRow(null, nextB);
        nextB = null;
      }
      long endTime = System.nanoTime();
      execTime += (endTime - startTime);
      return row;
    }
    long endTime = System.nanoTime();
    execTime += (endTime - startTime);
    return null;
  }

  private Row buildRow(Row rowA, Row rowB) {
    if (joinByTime) {
      long timestamp;
      Object[] values = new Object[header.getFieldSize()];
      if (rowA != null && rowB != null) {
        writeToNewRow(values, rowA);
        writeToNewRow(values, rowB);
        timestamp = rowA.getKey();
      } else if (rowA != null) {
        writeToNewRow(values, rowA);
        timestamp = rowA.getKey();
      } else {
        writeToNewRow(values, rowB);
        timestamp = rowB.getKey();
      }
      return new Row(header, timestamp, values);
    }
    if (joinByOrdinal) {
      Object[] values = new Object[header.getFieldSize()];
      if (rowA != null && rowB != null) {
        writeToNewRow(values, rowA);
        writeToNewRow(values, rowB);
      } else if (rowA != null) {
        writeToNewRow(values, rowA);
      } else {
        writeToNewRow(values, rowB);
      }
      return new Row(header, values);
    }
    return null;
  }

  private void writeToNewRow(Object[] values, Row row) {
    List<Field> fields = row.getHeader().getFields();
    for (int i = 0; i < fields.size(); i++) {
      if (row.getValue(i) == null) {
        continue;
      }
      values[fieldIndices.get(fields.get(i))] = row.getValue(i);
    }
  }

  @Override
  public Batch nextBatch() throws PhysicalException {
    long startTime = System.nanoTime();
    if (!hasNextBatch()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    if (streamA.hasNextBatch()) {
      batchA = streamA.nextBatch();
    }
    if (streamB.hasNextBatch()) {
      batchB = streamB.nextBatch();
    }
    double[] doubles = null;
    int[] ints = null;
    long[] keyInt = null;
    long[] keyDouble = null;

    if (batchA.getDataType()== DataType.DOUBLE) {
      doubles = (double[]) batchA.getBatch();
      keyDouble = (long[]) batchA.getKeys();
    } else {
      ints = (int[]) batchA.getBatch();
      keyInt = (long[]) batchA.getKeys();
    }

    if (batchB.getDataType()== DataType.DOUBLE) {
      doubles = (double[]) batchB.getBatch();
      keyDouble = (long[]) batchA.getKeys();
    } else {
      ints = (int[]) batchB.getBatch();
      keyInt = (long[]) batchA.getKeys();
    }

    int pos =0;
    int i=0,j=0;
    for(;i<keyInt.length;i++) {
      long key = keyInt[i];
      LongVector veckey1 = LongVector.broadcast(SPECIES_LONG, key); // Fills the vector with the value
      for (;j<keyDouble.length;j+=SPECIES_DOUBLE.length()) {
        LongVector veckey2 = LongVector.fromArray(SPECIES_LONG, keyDouble, j);
        VectorMask<Long> mask = veckey1.compare(VectorOperators.EQ, veckey2);
        if (mask.anyTrue()) {
          for (int p = 0; p < mask.length(); p++) {
            int tmp = mask.laneIsSet(p) ? 1 : 0;
            doubleResult[pos] = doubles[j+p];
            intResult[pos] = ints[j+p];
            keyResult[pos] = key;
            pos+=tmp;
          }
          break;
        }
      }
    }

    batchResult.setKeys(keyResult);
    batchResult.setHeader(header);
    batchResult.setInts(intResult);
    batchResult.setDoubles(doubleResult);
    batchResult.setBitmap(batchA.getBitmap());

    long endTime = System.nanoTime();
    execTime += (endTime - startTime);

    return batchResult;
  }

  @Override
  public boolean hasNextBatch() throws PhysicalException {
    return streamA.hasNextBatch() && streamB.hasNextBatch();
  }

  @Override
  public String printExecTime() throws PhysicalException {
    System.out.println(streamA.printExecTime());
    System.out.println(streamB.printExecTime());
    return "Join Exec Time: " + execTime;
  }
}
