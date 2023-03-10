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
package cn.edu.tsinghua.iginx.filesystem;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.filesystem.exec.Executor;
import cn.edu.tsinghua.iginx.filesystem.filesystem.IFileReader;
import cn.edu.tsinghua.iginx.filesystem.filesystem.entity.DefaultFileReader;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.wrapper.FilePath;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileSystem implements IStorage {
    private static final String STORAGE_ENGINE = "filesystem";
    public static final int MAXFILESIZE = 100_000_000;
    private static final Logger logger = LoggerFactory.getLogger(FileSystem.class);
    private final StorageEngineMeta meta;
    private Executor executor;

    public FileSystem(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
    }

    private boolean testConnection() {
        // may fix it when add the remote filesystem
        return true;
    }


    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() < 1) {
            return new TaskExecuteResult(new NonExecutablePhysicalTaskException("storage physical task should have one more operators"));
        }
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();
        boolean isDummyStorageUnit = task.isDummyStorageUnit();
        if (op.getType() == OperatorType.Project) {
            Project project = (Project) op;
            Filter filter;
            if (operators.size() == 2) {
                filter = ((Select) operators.get(1)).getFilter();
            } else {
                FragmentMeta fragment = task.getTargetFragment();
                filter = new AndFilter(Arrays.asList(
                        new KeyFilter(Op.GE, fragment.getTimeInterval().getStartTime()),
                        new KeyFilter(Op.L, fragment.getTimeInterval().getEndTime())));
            }
            return executor.executeProjectTask(
                    project.getPatterns(),
                    project.getTagFilter(),
                    filter,
                    storageUnit,
                    isDummyStorageUnit);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executor.executeInsertTask(
                    insert,
                    storageUnit);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executor.executeDeleteTask(
                    delete,
                    storageUnit);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    @Override
    public List<Timeseries> getTimeSeries() throws PhysicalException {
        return executor.getTimeSeriesOfStorageUnit("*");
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        return executor.getBoundaryOfStorage();
    }

    @Override
    public void release() throws PhysicalException {
        executor.close();
    }



    private TaskExecuteResult executeInsertTask(String storageUnit, Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertRowRecords((RowDataView) dataView, storageUnit);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in influxdb failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private Exception insertRowRecords(RowDataView data, String storageUnit) {
        // fix it 如果有远程文件系统则需要server
        IFileReader fileSystem = new DefaultFileReader();
        if (fileSystem == null) {
            return new PhysicalTaskExecuteFailureException("get fileSystem failure!");
        }

        for (int i = 0; i < data.getPathNum(); i++) {
            schemas.add(new InfluxDBSchema(data.getPath(i), data.getTags(i)));
        }

        List<Point> points = new ArrayList<>();
        for (int i = 0; i < data.getTimeSize(); i++) {
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            for (int j = 0; j < data.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    switch (data.getDataType(j)) {
                        case BOOLEAN:
                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (boolean) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
                            break;
                        case INTEGER:
                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (int) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
                            break;
                        case LONG:
                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (long) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
                            break;
                        case FLOAT:
                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (float) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
                            break;
                        case DOUBLE:
                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (double) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
                            break;
                        case BINARY:
                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), new String((byte[]) data.getValue(i, index))).time(data.getKey(i), WRITE_PRECISION));
                            break;
                    }
                    index++;
                }
            }
        }
        try {
            logger.info("开始数据写入");
            fileSystem.write();
            client.getWriteApiBlocking().writePoints(bucket.getId(), organization.getId(), points);
        } catch (Exception e) {
            logger.error("encounter error when write points to influxdb: ", e);
        } finally {
            logger.info("数据写入完毕！");
        }
        return null;
    }
}



























