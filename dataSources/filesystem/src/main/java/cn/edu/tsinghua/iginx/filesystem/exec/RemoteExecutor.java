package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.filesystem.thrift.TagFilterType;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(RemoteExecutor.class);

    private static final int SUCCESS_CODE = 200;

    private final TTransport transport;

    private final FileSystemService.Iface client;

    public RemoteExecutor(String ip, int port) throws TTransportException {
        this.transport = new TSocket(ip, port);
        if (!transport.isOpen()) {
            transport.open();
        }

        this.client = new FileSystemService.Client(new TBinaryProtocol(transport));
    }

    @Override
    public TaskExecuteResult executeProjectTask(
            Project project, byte[] filter, String storageUnit, boolean isDummyStorageUnit) {
        ProjectReq req = new ProjectReq(storageUnit, isDummyStorageUnit, project.getPatterns());
        if (project.getTagFilter() != null) {
            req.setTagFilter(constructRawTagFilter(project.getTagFilter()));
        }
        if (filter != null && !filter.equals("")) {
            req.setFilter(filter);
        }
        try {
            ProjectResp resp = client.executeProject(req);
            if (resp.getStatus().code == SUCCESS_CODE) {
                FileDataHeader fileDataHeader = resp.getHeader();
                List<DataType> dataTypes = new ArrayList<>();
                List<Field> fields = new ArrayList<>();
                for (int i = 0; i < fileDataHeader.getNamesSize(); i++) {
                    DataType dataType =
                            DataTypeUtils.strToDataType(fileDataHeader.getTypes().get(i));
                    dataTypes.add(dataType);
                    fields.add(
                            new Field(
                                    fileDataHeader.getNames().get(i),
                                    dataType,
                                    fileDataHeader.getTagsList().get(i)));
                }
                Header header =
                        fileDataHeader.hasTime ? new Header(Field.KEY, fields) : new Header(fields);

                List<Row> rowList = new ArrayList<>();
                resp.getRows()
                        .forEach(
                                fileDataRow -> {
                                    Object[] values = new Object[dataTypes.size()];
                                    Bitmap bitmap =
                                            new Bitmap(dataTypes.size(), fileDataRow.getBitmap());
                                    ByteBuffer valuesBuffer =
                                            ByteBuffer.wrap(fileDataRow.getRowValues());
                                    for (int i = 0; i < dataTypes.size(); i++) {
                                        if (bitmap.get(i)) {
                                            values[i] =
                                                    ByteUtils.getValueFromByteBufferByDataType(
                                                            valuesBuffer, dataTypes.get(i));
                                        } else {
                                            values[i] = null;
                                        }
                                    }

                                    if (fileDataRow.isSetTimestamp()) {
                                        rowList.add(
                                                new Row(
                                                        header,
                                                        fileDataRow.getTimestamp(),
                                                        values));
                                    } else {
                                        rowList.add(new Row(header, values));
                                    }
                                });
                RowStream rowStream = new Table(header, rowList);
                return new TaskExecuteResult(rowStream, null);
            } else {
                return new TaskExecuteResult(
                        null, new PhysicalException("execute remote project task error"));
            }
        } catch (TException e) {
            return new TaskExecuteResult(null, new PhysicalException(e));
        }
    }

    @Override
    public TaskExecuteResult executeInsertTask(Insert insert, String storageUnit) {
        List<String> paths = new ArrayList<>();
        List<String> types = new ArrayList<>();
        DataView dataView = insert.getData();
        List<Map<String, String>> tagsList = new ArrayList<>();
        for (int i = 0; i < dataView.getPathNum(); i++) {
            paths.add(dataView.getPath(i));
            types.add(dataView.getDataType(i).toString());
            tagsList.add(dataView.getTags(i) == null ? new HashMap<>() : dataView.getTags(i));
        }

        long[] times = new long[dataView.getTimeSize()];
        for (int i = 0; i < dataView.getTimeSize(); i++) {
            times[i] = dataView.getKey(i);
        }

        Pair<List<ByteBuffer>, List<ByteBuffer>> pair;
        if (dataView.getRawDataType() == RawDataType.Row
                || dataView.getRawDataType() == RawDataType.NonAlignedRow) {
            pair = compressRowData(dataView);
        } else {
            pair = compressColData(dataView);
        }

        FileDataRawData fileDataRawData =
                new FileDataRawData(
                        paths,
                        tagsList,
                        ByteBuffer.wrap(ByteUtils.getByteArrayFromLongArray(times)),
                        pair.getK(),
                        pair.getV(),
                        types,
                        dataView.getRawDataType().toString());

        InsertReq req = new InsertReq(storageUnit, fileDataRawData);
        try {
            Status status = client.executeInsert(req);
            if (status.code == SUCCESS_CODE) {
                return new TaskExecuteResult(null, null);
            } else {
                return new TaskExecuteResult(
                        null, new PhysicalException("execute remote insert task error"));
            }
        } catch (TException e) {
            return new TaskExecuteResult(null, new PhysicalException(e));
        }
    }

    @Override
    public TaskExecuteResult executeDeleteTask(Delete delete, String storageUnit) {
        List<String> paths = delete.getPatterns();
        List<TimeRange> timeRanges = delete.getTimeRanges();
        TagFilter tagFilter = delete.getTagFilter();
        DeleteReq req = new DeleteReq(storageUnit, paths);
        if (tagFilter != null) {
            req.setTagFilter(constructRawTagFilter(tagFilter));
        }
        if (timeRanges != null) {
            List<FileSystemTimeRange> fileSystemTimeRange = new ArrayList<>();
            timeRanges.forEach(
                    timeRange ->
                            fileSystemTimeRange.add(
                                    new FileSystemTimeRange(
                                            timeRange.getBeginTime(),
                                            timeRange.isIncludeBeginTime(),
                                            timeRange.getEndTime(),
                                            timeRange.isIncludeEndTime())));
            req.setTimeRanges(fileSystemTimeRange);
        }

        try {
            Status status = client.executeDelete(req);
            if (status.code == SUCCESS_CODE) {
                return new TaskExecuteResult(null, null);
            } else {
                return new TaskExecuteResult(
                        null, new PhysicalException("execute remote delete task error"));
            }
        } catch (TException e) {
            return new TaskExecuteResult(null, new PhysicalException(e));
        }
    }

    @Override
    public List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit)
            throws PhysicalException {
        try {
            GetTimeSeriesOfStorageUnitResp resp = client.getTimeSeriesOfStorageUnit(storageUnit);
            List<Timeseries> timeSeriesList = new ArrayList<>();
            resp.getPathList()
                    .forEach(
                            ts ->
                                    timeSeriesList.add(
                                            new Timeseries(
                                                    ts.getPath(),
                                                    DataTypeUtils.strToDataType(ts.getDataType()),
                                                    ts.getTags())));
            return timeSeriesList;
        } catch (TException e) {
            throw new PhysicalException("encounter error when getTimeSeriesOfStorageUnit ", e);
        }
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix)
            throws PhysicalException {
        try {
            GetStorageBoundryResp resp = client.getBoundaryOfStorage(prefix);
            return new Pair<>(
                    new TimeSeriesInterval(resp.getStartTimeSeries(), resp.getEndTimeSeries()),
                    new TimeInterval(resp.getStartTime(), resp.getEndTime()));
        } catch (TException e) {
            throw new PhysicalException("encounter error when getBoundaryOfStorage ", e);
        }
    }

    @Override
    public void close() throws PhysicalException {
        if (transport != null && transport.isOpen()) {
            transport.close();
        }
    }

    private RawTagFilter constructRawTagFilter(TagFilter tagFilter) {
        RawTagFilter filter  = null;
        switch (tagFilter.getType()) {
            case Base:
                {
                    BaseTagFilter baseTagFilter = (BaseTagFilter) tagFilter;
                    filter = new RawTagFilter(TagFilterType.Base);
                    filter.setKey(baseTagFilter.getTagKey());
                    filter.setValue(baseTagFilter.getTagValue());
                   break;
                }
            case WithoutTag:
                {
                    filter = new RawTagFilter(TagFilterType.WithoutTag);
                    break;
                }
            case BasePrecise:
                {
                    BasePreciseTagFilter basePreciseTagFilter = (BasePreciseTagFilter) tagFilter;
                    filter = new RawTagFilter(TagFilterType.BasePrecise);
                    filter.setTags(basePreciseTagFilter.getTags());
                    break;
                }
            case Precise:
                {
                    PreciseTagFilter preciseTagFilter = (PreciseTagFilter) tagFilter;
                    filter = new RawTagFilter(TagFilterType.Precise);
                    filter.setChildren(preciseTagFilter.getChildren()
                        .stream()
                        .map(this::constructRawTagFilter)
                        .collect(Collectors.toList()));
                    break;
                }
            case And:
                {
                    AndTagFilter andTagFilter = (AndTagFilter) tagFilter;
                    filter = new RawTagFilter(TagFilterType.And);
                    filter.setChildren(andTagFilter.getChildren()
                        .stream()
                        .map(this::constructRawTagFilter)
                        .collect(Collectors.toList()));
                    break;
                }
            case Or:
                {
                    OrTagFilter orTagFilter = (OrTagFilter) tagFilter;
                    filter = new RawTagFilter(TagFilterType.Or);
                    filter.setChildren(orTagFilter.getChildren()
                        .stream()
                        .map(this::constructRawTagFilter)
                        .collect(Collectors.toList()));
                    break;
                }
            default:
                {
                    logger.error("unknown tag filter type: {}", tagFilter.getType());
                }
        }
        return filter;
    }

    private Pair<List<ByteBuffer>, List<ByteBuffer>> compressColData(DataView dataView) {
        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();

        for (int i = 0; i < dataView.getPathNum(); i++) {
            DataType dataType = dataView.getDataType(i);
            BitmapView bitmapView = dataView.getBitmapView(i);
            Object[] values = new Object[dataView.getTimeSize()];

            int index = 0;
            for (int j = 0; j < dataView.getTimeSize(); j++) {
                if (bitmapView.get(j)) {
                    values[j] = dataView.getValue(i, index);
                    index++;
                } else {
                    values[j] = null;
                }
            }
            valueBufferList.add(ByteUtils.getColumnByteBuffer(values, dataType));
            bitmapBufferList.add(ByteBuffer.wrap(bitmapView.getBitmap().getBytes()));
        }
        return new Pair<>(valueBufferList, bitmapBufferList);
    }

    private Pair<List<ByteBuffer>, List<ByteBuffer>> compressRowData(DataView dataView) {
        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < dataView.getPathNum(); i++) {
            dataTypeList.add(dataView.getDataType(i));
        }

        for (int i = 0; i < dataView.getTimeSize(); i++) {
            BitmapView bitmapView = dataView.getBitmapView(i);
            Object[] values = new Object[dataView.getPathNum()];

            int index = 0;
            for (int j = 0; j < dataView.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    values[j] = dataView.getValue(i, index);
                    index++;
                } else {
                    values[j] = null;
                }
            }
            valueBufferList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
            bitmapBufferList.add(ByteBuffer.wrap(bitmapView.getBitmap().getBytes()));
        }
        return new Pair<>(valueBufferList, bitmapBufferList);
    }
}