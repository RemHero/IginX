package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.query.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FuncUT {

    @Test
    public void readLocalFile() throws IOException, PhysicalException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz.txt";
        int index = 0;
        byte[] resbyte = new byte[100];
        IFileOperator fileSystem = new DefaultFileOperator();
        List<Record> res = fileSystem.read(new File(path));
        for (Record record : res) {
            resbyte[index++] = (byte) record.getRawData();
            System.out.println(record.getRawData());
        }
        System.out.println(new String(resbyte));

        List<List<Record>> result = new ArrayList<>();
        List<FilePath> pathList = new ArrayList<>();
        FilePath seriesOperator = new FilePath(null, path);
        pathList.add(seriesOperator);
        result.add(res);
        RowStream rowStream = new FileSystemQueryRowStream(result, pathList, null);

        System.out.println(rowStream.getHeader());
        while (rowStream.hasNext()) {
            System.out.println(rowStream.next());
        }
    }

}
