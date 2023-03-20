package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filesystem.FileSystem;
import cn.edu.tsinghua.iginx.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemImpl;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FuncUT {

    @Test
    public void testReadFile() throws IOException, PhysicalException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz.txt";
        int index = 0;
        List<byte[]> resbyte = new ArrayList<>();
        FileSystemImpl fileSystem = new FileSystemImpl();
        List<Record> res = fileSystem.readFile(new File(path));
        for (Record record : res) {
            resbyte.add((byte[]) record.getRawData());
            System.out.println(new String(resbyte.get(index++)));
        }

        List<List<Record>> result = new ArrayList<>();
        List<FilePath> pathList = new ArrayList<>();
        FilePath seriesOperator = new FilePath(null, path);
        pathList.add(seriesOperator);
        result.add(res);
        RowStream rowStream = new FileSystemQueryRowStream(result, pathList, null, null);

        System.out.println(rowStream.getHeader());
//        while (rowStream.hasNext()) {
//            System.out.println(rowStream.next());
//        }
    }

    @Test
    public void testExecuteQueryTask() throws IOException, PhysicalException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz.txt";
        int index = 0;
        List<byte[]> resbyte = new ArrayList<>();
        LocalExecutor localExecutor = new LocalExecutor();
        TaskExecuteResult res = localExecutor.executeQueryTask(null, new ArrayList<>(Collections.singleton(path)), null, null);

        RowStream rowStream = res.getRowStream();
        while (rowStream.hasNext()) {
            System.out.println(new String((byte[]) rowStream.next().getValue(0)));
        }
    }

    @Test
    public void testInsertRowRecords() throws IOException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz2.txt";
        FileSystemImpl fileSystem = new FileSystemImpl();
        List<List<Record>> valList = new ArrayList<>();
        List<File> fileList = new ArrayList<>();
        List<Boolean> ifAppend = new ArrayList<>();

        fileList.add(new File(path));

        valList.add(new ArrayList<Record>() {{
            long key = TimeUtils.MIN_AVAILABLE_TIME;
            add(new Record(key++, "lhz never give up!\n".getBytes()));
            add(new Record(key++, "happy every day!\n".getBytes()));
        }});

        ifAppend.add(false);
        fileSystem.writeFiles(fileList, valList, ifAppend);
    }

    @Test
    public void testDeleteFiles() throws IOException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz2.txt";
        File file = new File(path);
        FileSystemImpl fileSystem = new FileSystemImpl();
        fileSystem.deleteFiles(Collections.singletonList(file));
    }

    @Test
    public void testGetTimeSeriesOfStorageUnit() throws IOException, PhysicalException {
        String path = "src";
        LocalExecutor localExecutor = new LocalExecutor();
        List<Timeseries> pathList = localExecutor.getTimeSeriesOfStorageUnit(path);
        for (Timeseries timeseries : pathList) {
            System.out.println(timeseries.getPath());
        }
    }

    @Test
    public void testGetBoundaryOfStorage() throws IOException, PhysicalException {
        String path = "src";
        LocalExecutor localExecutor = new LocalExecutor();
        Pair<TimeSeriesRange, TimeInterval> res = localExecutor.getBoundaryOfStorage(path);
        System.out.println(res.k.getStartTimeSeries());
        System.out.println(res.k.getEndTimeSeries());
        System.out.println(res.v.getStartTime());
        System.out.println(res.v.getEndTime());
    }

    @Test
    public void testReadIginxFileByKey() throws IOException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz.iginx.parquet";
        FileSystemImpl fileSystem = new FileSystemImpl();
        List<Record> res = fileSystem.readFile(new File(path), 0, 100);
        for (Record record : res) {
            System.out.println(record.getRawData());
        }
    }
}
