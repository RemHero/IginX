package cn.edu.tsinghua.iginx.filesystem.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.filesystem.IFileReader;
import cn.edu.tsinghua.iginx.filesystem.filesystem.entity.DefaultFileReader;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.wrapper.FilePath;
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
    public TaskExecuteResult executeProjectTask(Project project, Filter filter, String storageUnit, boolean isDummyStorageUnit) {
        List<String> series = project.getPatterns();
        TagFilter tagFilter = project.getTagFilter();

        if(isDummyStorageUnit) {

        }

        return executeQueryTask(storageUnit, series, tagFilter, filter);
    }

    private TaskExecuteResult executeQueryTask(String storageUnit, List<String> series, TagFilter tagFilter, Filter filter) {
        try {
            List<FilePath> pathList = new ArrayList<>();
            List<List<Record>> result = new ArrayList<>();
            // fix it 如果有远程文件系统则需要server
            IFileReader fileSystem = new DefaultFileReader();
            logger.info("[Query] execute query file: " + project.getPatterns());
            for(String path : project.getPatterns()) {
                // not put storageUnit in front of path, may fix it
                FilePath seriesOperator = new FilePath(null, path);
                pathList.add(seriesOperator);
                result.add(fileSystem.read(new File(seriesOperator.getFilePath())));
            }
            RowStream rowStream = new FileSystemQueryRowStream(result, pathList, project);
            return new TaskExecuteResult(rowStream);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in iotdb12 failure", e));
        }
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
