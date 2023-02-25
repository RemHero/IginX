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
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FileSystem implements IStorage {
    private static final String STORAGE_ENGINE = "filesystem";
    private final StorageEngineMeta meta;

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
        // fix it when add the remote filesystem
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
                filter = new AndFilter(Arrays.asList(new KeyFilter(Op.GE, fragment.getTimeInterval().getStartTime()), new KeyFilter(Op.L, fragment.getTimeInterval().getEndTime())));
            }
            // fix it
//            return isDummyStorageUnit ? executeQueryHistoryTask(task.getTargetFragment().getTsInterval(), project, filter) : executeQueryTask(storageUnit, project, filter);
            return executeQueryTask(storageUnit, project, filter);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
//            return executeInsertTask(storageUnit, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
//            return executeDeleteTask(storageUnit, delete);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    @Override
    public List<Timeseries> getTimeSeries() throws PhysicalException {
        return null;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        return null;
    }

    @Override
    public void release() throws PhysicalException {

    }

    private TaskExecuteResult executeQueryTask(String storageUnit, Project project, Filter filter) {
        try {
            StringBuilder builder = new StringBuilder();
            for (String path : project.getPatterns()) {
                builder.append(path);
                builder.append(',');
            }
            String statement = String.format(QUERY_DATA, builder.deleteCharAt(builder.length() - 1).toString(), storageUnit, FilterTransformer.toString(filter));
            logger.info("[Query] execute query: " + statement);
            RowStream rowStream = new ClearEmptyRowStreamWrapper(new IoTDBQueryRowStream(sessionPool.executeQueryStatement(statement), true, project));
            return new TaskExecuteResult(rowStream);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in iotdb12 failure", e));
        }
    }
}