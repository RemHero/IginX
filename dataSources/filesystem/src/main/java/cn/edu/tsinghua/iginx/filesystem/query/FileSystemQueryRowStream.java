package cn.edu.tsinghua.iginx.filesystem.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.filesystem.tools.SeriesOperator;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;

public class FileSystemQueryRowStream implements RowStream {
    private final List<byte[]> dataset;

    private final Header header;

    public FileSystemQueryRowStream(List<byte[]> result, List<SeriesOperator> pathList, Project project) {
        // fix it 先假设查询的全是文件
        dataset = result;
        Field time = Field.KEY;
        List<Field> fields = new ArrayList<>();

        for(SeriesOperator series : pathList) {
            Field field = new Field(series.getFilePath(), DataType.BINARY, null);
            fields.add(field);
        }

        this.header = new Header(time, fields);
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
        return false;
    }

    @Override
    public Row next() throws PhysicalException {
        return null;
    }
}
