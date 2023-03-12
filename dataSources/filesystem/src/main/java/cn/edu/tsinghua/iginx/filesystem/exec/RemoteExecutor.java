package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.thrift.FileSystemService;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.parquet.thrift.ProjectReq;
import cn.edu.tsinghua.iginx.parquet.thrift.ProjectResp;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RemoteExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(RemoteExecutor.class);

    private final static int SUCCESS_CODE = 200;

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
    public TaskExecuteResult executeProjectTask(Project project, Filter filter, String storageUnit, boolean isDummyStorageUnit) {
        ProjectReq req = new ProjectReq(storageUnit, isDummyStorageUnit, project.getPatterns());
        if (project.getTagFilter() != null) {
            req.setTagFilter(constructRawTagFilter(project.getTagFilter()));
        }
        if (filter != null && !filter.equals("")) {
            req.setFilter(filter.toString());
        }
        try {
            ProjectResp resp = client.executeProject(req);
            if (resp.getStatus().code == SUCCESS_CODE) {
    }

    @Override
    public TaskExecuteResult executeInsertTask(Insert insert, String storageUnit) {
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
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage() throws PhysicalException {
        return null;
    }

    @Override
    public void close() throws PhysicalException {

    }
}
