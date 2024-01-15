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

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.combineMultipleColumns;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.utils.Pair;
import jdk.incubator.vector.*;

import java.util.ArrayList;
import java.util.List;

public class RowTransformLazyStream extends UnaryLazyStream {

  private final RowTransform rowTransform;

  private final List<Pair<RowMappingFunction, FunctionParams>> functionAndParamslist;

  private Row nextRow;

  private Header header;

  final VectorSpecies<Double> SPECIES_DOUBLE = DoubleVector.SPECIES_PREFERRED;
  final VectorSpecies<Integer> SPECIES_INTEGER = IntVector.SPECIES_PREFERRED;
  final VectorSpecies<Long> SPECIES_LONG = LongVector.SPECIES_PREFERRED;
  private double[] result = new double[Batch.BATCH_SIZE];
  private long execTime = 0;

  public RowTransformLazyStream(RowTransform rowTransform, RowStream stream) {
    super(stream);
    this.rowTransform = rowTransform;
    this.functionAndParamslist = new ArrayList<>();
    rowTransform
        .getFunctionCallList()
        .forEach(
            functionCall -> {
              this.functionAndParamslist.add(
                  new Pair<>(
                      (RowMappingFunction) functionCall.getFunction(), functionCall.getParams()));
            });
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      if (nextRow == null) {
        nextRow = calculateNext();
      }
      header = nextRow == null ? Header.EMPTY_HEADER : nextRow.getHeader();
    }
    return header;
  }

  private Row calculateNext() throws PhysicalException {
    while (stream.hasNext()) {
      List<Row> columnList = new ArrayList<>();
      functionAndParamslist.forEach(
          pair -> {
            RowMappingFunction function = pair.k;
            FunctionParams params = pair.v;
            Row column = null;
            try {
              // 分别计算每个表达式得到相应的结果
              column = function.transform(stream.next(), params);
            } catch (Exception e) {
              try {
                throw new PhysicalTaskExecuteFailureException(
                    "encounter error when execute row mapping function "
                        + function.getIdentifier()
                        + ".",
                    e);
              } catch (PhysicalTaskExecuteFailureException ex) {
                throw new RuntimeException(ex);
              }
            }
            if (column != null) {
              columnList.add(column);
            }
          });
      // 如果计算结果都不为空，将计算结果合并成一行
      if (columnList.size() == functionAndParamslist.size()) {
        return combineMultipleColumns(columnList);
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      nextRow = calculateNext();
    }
    return nextRow != null;
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
    long startTime = System.nanoTime();
    if (!stream.hasNextBatch()) {
      return null;
    }
    Batch batch = stream.nextBatch();

    double[] doubles = batch.getDoubles();
    int[] ints = batch.getInts();
    double[] doubles2 = new double[ints.length];

//    for (int i = 0; i < ints.length; i++) {
//      doubles2[i] = ints[i];
//    }


    int length = SPECIES_INTEGER.length();

    // 使用向量操作处理数组的每个部分
    for (int i = 0; i < ints.length - length; i += length) {
      // 加载一个整型向量
      var intVector = IntVector.fromArray(SPECIES_INTEGER, ints, i);

      // 转换为双精度浮点向量
      DoubleVector  doubleVector = intVector.convert(VectorOperators.I2B, 0).reinterpretAsDoubles();

      // 存储双精度浮点向量到数组
      doubleVector.intoArray(doubles2, i);
    }

    for (int i = 0; i < doubles.length; i += SPECIES_DOUBLE.length()) {
      DoubleVector doubleVec = DoubleVector.fromArray(SPECIES_DOUBLE, doubles, i);
      DoubleVector intVec = DoubleVector.fromArray(SPECIES_DOUBLE, doubles2, i);

      DoubleVector calc = doubleVec.mul(intVec).add(doubleVec.div(intVec)).div(intVec).mul(doubleVec);
      calc.intoArray(result, i);
    }
    long endTime = System.nanoTime();
    execTime += (endTime - startTime);

    return new Batch(header, batch.getKeys(), result);
  }

  @Override
  public boolean hasNextBatch() throws PhysicalException {
    return stream.hasNextBatch();
  }

  @Override
  public String printExecTime() throws PhysicalException {
    System.out.println(stream.printExecTime());
    return "RowTransform Exec Time: " + execTime;
  }
}
