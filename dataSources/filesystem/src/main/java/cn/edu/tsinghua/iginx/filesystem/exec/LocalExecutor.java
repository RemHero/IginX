package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemImpl;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class LocalExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

    @Override
    public TaskExecuteResult executeProjectTask(Project project, String filter, String storageUnit, boolean isDummyStorageUnit) {
        List<String> series = project.getPatterns();
        TagFilter tagFilter = project.getTagFilter();
        if (isDummyStorageUnit) {
//            return executeDummyProjectTask(storageUnit, series, tagFilter, filter);
        }
//        return executeQueryTask(storageUnit, series, tagFilter, filter);
        return null;
    }

    private TaskExecuteResult executeQueryTask(String storageUnit, List<String> series, TagFilter tagFilter, Filter filter) {
        try {
            List<FilePath> pathList = new ArrayList<>();
            List<List<Record>> result = new ArrayList<>();
            // fix it 如果有远程文件系统则需要server
            FileSystemImpl fileSystem = new FileSystemImpl();
            logger.info("[Query] execute query file: " + series);
            for (String path : series) {
                // not put storageUnit in front of path, may fix it
                FilePath seriesOperator = new FilePath(null, path);
                pathList.add(seriesOperator);
                result.add(fileSystem.readFile(new File(seriesOperator.getFilePath())));
            }
            RowStream rowStream = new FileSystemQueryRowStream(result, pathList, tagFilter, filter);
            return new TaskExecuteResult(rowStream);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in iotdb12 failure", e));
        }
    }

    private TaskExecuteResult executeDummyProjectTask(String storageUnit, List<String> paths, TagFilter tagFilter,
                                                      Filter filter) {
        return null;
    }

    @Override
    public TaskExecuteResult executeInsertTask(Insert insert, String storageUnit) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertRowRecords((RowDataView) dataView, storageUnit);
                break;
            case Column:
            case NonAlignedColumn:
//                e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in influxdb failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private Exception insertRowRecords(RowDataView data, String storageUnit) {
        // fix it 如果有远程文件系统则需要server
//        FileSystemImpl fileSystem = new FileSystemImpl();
//        if (fileSystem == null) {
//            return new PhysicalTaskExecuteFailureException("get fileSystem failure!");
//        }
//
//        for (int i = 0; i < data.getPathNum(); i++) {
//            schemas.add(new InfluxDBSchema(data.getPath(i), data.getTags(i)));
//        }
//
//        List<Point> points = new ArrayList<>();
//        for (int i = 0; i < data.getTimeSize(); i++) {
//            BitmapView bitmapView = data.getBitmapView(i);
//            int index = 0;
//            for (int j = 0; j < data.getPathNum(); j++) {
//                if (bitmapView.get(j)) {
//                    switch (data.getDataType(j)) {
//                        case BOOLEAN:
//                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (boolean) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
//                            break;
//                        case INTEGER:
//                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (int) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
//                            break;
//                        case LONG:
//                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (long) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
//                            break;
//                        case FLOAT:
//                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (float) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
//                            break;
//                        case DOUBLE:
//                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), (double) data.getValue(i, index)).time(data.getKey(i), WRITE_PRECISION));
//                            break;
//                        case BINARY:
//                            points.add(Point.measurement(schema.getMeasurement()).addTags(schema.getTags()).addField(schema.getField(), new String((byte[]) data.getValue(i, index))).time(data.getKey(i), WRITE_PRECISION));
//                            break;
//                    }
//                    index++;
//                }
//            }
//        }
//        try {
//            logger.info("开始数据写入");
//            fileSystem.write();
//            client.getWriteApiBlocking().writePoints(bucket.getId(), organization.getId(), points);
//        } catch (Exception e) {
//            logger.error("encounter error when write points to influxdb: ", e);
//        } finally {
//            logger.info("数据写入完毕！");
//        }
        return null;
    }

    @Override
    public TaskExecuteResult executeDeleteTask(Delete delete, String storageUnit) {
        return null;
    }

    @Override
    public List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit) throws PhysicalException {
        return null;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        return null;
    }

    @Override
    public void close() throws PhysicalException {

    }
}
