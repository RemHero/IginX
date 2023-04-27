package cn.edu.tsinghua.iginx.integration.tmp;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;

import cn.edu.tsinghua.iginx.session.Session;

public class testFilesystemReadLargeFile {
    private static final int BLOCK_SIZE = 1024 * 1024;  // 1MB
    private static final int FILE_SIZE = 100*1024 * BLOCK_SIZE;  // 1GB

    @Test
    public void genF() {
        writeFile(1024);
    }

    private void writeFile(int N) {
        if (N < 1) {
            System.out.println("writeFile please input one integer greater than 0");
            return;
        }

        System.out.println("writeFile start >>>");
        RandomAccessFile file = null;
        try {
            System.out.println("Will write " + N + "MB data ...");
            final String FILE_NAME = "D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\" + "1GB";
            File f = new File(FILE_NAME);
            if (f.exists()) {
                f.delete();
            }
            file = new RandomAccessFile(FILE_NAME, "rw");
            final int ONE_G_LENGTH = BLOCK_SIZE; // 1MB
            byte[] b = new byte[ONE_G_LENGTH]; // 1 MB
            b[0] = '0';
            b[b.length - 1] = '1';
            for (int i = 0; i < N; ++i) {
                file.write(b);
                file.seek(file.length());
                System.out.println("write " + (i + 1) + "MB data");
            }
            file.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (file != null) {
                try {
                    file.close();
                    file = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("writeFile end <<<");
    }

    protected String defaultTestHost = "127.0.0.1";
    protected int defaultTestPort = 6888;
    protected String defaultTestUser = "root";
    protected String defaultTestPass = "root";

    @Test
    public void testReadLargeFile() throws SessionException, ExecutionException {
        // 在这里执行函数的代码
        SessionPool session = new SessionPool(defaultTestHost,
                defaultTestPort,
                defaultTestUser,
                defaultTestPass,5);
//        Session session =
//                new Session(
//                        defaultTestHost,
//                        defaultTestPort,
//                        defaultTestUser,
//                        defaultTestPass);
//        session.openSession();

//        String stmt = "select * from largefiletest.100GB where key>%s and key<%s";
        String fileName = "10GB";
        File file = new File("D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\"+fileName);
        long size = file.length();
        long offset = 0;
        long batchSize = 1024*500;

        long startTime = System.currentTimeMillis();
        while (offset < size) {
            long start = offset;
            long end = Math.min(offset + batchSize, size);

//            String query = String.format(stmt,start,end);
//            session.executeSql(query);
//            SessionQueryDataSet sessionQueryDataSet=
            session.queryData(Collections.singletonList("a."+fileName),start, end);

            offset += batchSize;
//            System.out.println("read File end <<<" + offset);
        }
        long endTime = System.currentTimeMillis();
        long totalTimeSeconds = (endTime - startTime) / 1000;
        System.out.println("Function took " + totalTimeSeconds + " seconds.");

    }
}