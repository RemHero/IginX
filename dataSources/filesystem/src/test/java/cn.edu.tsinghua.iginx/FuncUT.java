package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
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

}
