package cn.edu.tsinghua.iginx.filesystem.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.filesystem.wrapper.FilePath;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;

public class FileSystemQueryRowStream implements RowStream {
    private final Header header;
    private final List<List<Record>> rowData;
    private final int[] indices;
    private int hasMoreRecords;

    public FileSystemQueryRowStream(List<List<Record>> result, List<FilePath> pathList, Project project) {
        // fix it 先假设查询的全是NormalFile类型
        Field time = Field.KEY;
        List<Field> fields = new ArrayList<>();

        for(FilePath series : pathList) {
            Field field = new Field(series.getFilePath(), DataType.INTEGER);// fix it 先假设查询的全是NormalFile类型
            fields.add(field);
        }
        this.rowData = result;
        this.indices = new int[this.rowData.size()];
        this.header = new Header(time, fields);
        this.hasMoreRecords = this.rowData.size();
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
        return this.hasMoreRecords != 0;
    }

    @Override
    public Row next() throws PhysicalException {
        long timestamp = Long.MAX_VALUE;
        for (int i = 0; i < this.rowData.size(); i++) {
            int index = indices[i];
            List<Record> records = this.rowData.get(i);
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            Record record = records.get(index);
            timestamp = Math.min(record.getTimestamp(), timestamp);
        }
        if (timestamp == Long.MAX_VALUE) {
            return null;
        }
        Object[] values = new Object[rowData.size()];
        for (int i = 0; i < this.rowData.size(); i++) {
            int index = indices[i];
            List<Record> records = this.rowData.get(i);
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            Record record = records.get(index);
            if (record.getTimestamp() == timestamp) { // 考虑时间 ns may fix it
                DataType dataType = header.getField(i).getType();
                Object value = record.getRawData();
                if (dataType == DataType.BINARY) {
                    value = ((String) value).getBytes();
                }
                values[i] = value;
                indices[i]++;
                if (indices[i] == records.size()) {
                    hasMoreRecords--;
                }
            }
        }
        return new Row(header, timestamp, values);
    }
}
