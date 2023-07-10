package cn.edu.tsinghua.iginx.integration.tmp;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.filesystem.tools.MemoryPool;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import org.junit.Test;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iginx.session.Session;

public class testFilesystemReadLargeFile {
    private static final int BLOCK_SIZE = 1024 * 1024;  // 1MB
    private static final int FILE_SIZE = 100*1024 * BLOCK_SIZE;  // 1GB

    @Test
    public void genF() {
        writeFile(1024*25);
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
            final String FILE_NAME = "D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\" + "25GB";
            File f = new File(FILE_NAME);
            if (f.exists()) {
                f.delete();
            }
            file = new RandomAccessFile(FILE_NAME, "rw");
            final int ONE_G_LENGTH = BLOCK_SIZE; // 1MB

            for (int i = 0; i < N; ++i) {
                byte[] b = new byte[ONE_G_LENGTH]; // 1 MB
                SecureRandom random = new SecureRandom();
                random.nextBytes(b);
                b[0] = '0';
                b[b.length - 1] = '1';
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
    public void testReadLargeFileSingleThread() throws SessionException, ExecutionException, IOException, InterruptedException {
        String fileName = "10GB";
        File file = new File("D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\"+fileName);
        long size = file.length();
        long offset = 0;
        int batchSize = 1024*1024*100;

        long startTime = System.currentTimeMillis();
        while (offset < size) {
            getByteFromFile(file, offset, batchSize);
            offset += batchSize;
        }
        long endTime = System.currentTimeMillis();
        long totalTimeSeconds = (endTime - startTime) / 1000;
        System.out.println(" time "+ totalTimeSeconds + " seconds.");
    }

    @Test
    public void testReadLargeFile() throws SessionException, ExecutionException, IOException, InterruptedException {
        // 在这里执行函数的代码
        SessionPool session = new SessionPool(defaultTestHost,
                defaultTestPort,
                defaultTestUser,
                defaultTestPass,10);
//        Session session =
//                new Session(
//                        defaultTestHost,
//                        defaultTestPort,
//                        defaultTestUser,
//                        defaultTestPass);
//        session.openSession();

//        String stmt = "select * from largefiletest.100GB where key>%s and key<%s";

//        for (int tt=0;tt<100;tt+=5) {
            long startTime = System.currentTimeMillis();
            for(int xx =0;xx<1;xx++) {
                String fileName = "25GB";
                File file = new File("D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\"+fileName);
                long size = file.length();
                long offset = 0;
                long batchSize = 1024*1024*100;


                while (offset < size) {
                    long start = offset;
                    long end = Math.min(offset + batchSize, size);

//            String query = String.format(stmt,start,end);
//            session.executeSql(query);
                    SessionQueryDataSet sessionQueryDataSet=
                            session.queryData(Collections.singletonList("a."+fileName),start, end);

//                    if (!ifRight(start,sessionQueryDataSet,"D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\"+fileName)) {
//                        System.out.println("data not right " + start +" " + end);
//                        return;
//                    };
                    offset += batchSize;
//            System.out.println("read File end <<<" + offset);
                }
//            }
            long endTime = System.currentTimeMillis();
            long totalTimeSeconds = (endTime - startTime) / 1000;
            System.out.println(" time "+ totalTimeSeconds + " seconds.");
            Thread.sleep(1000);
                x=0;
        }
    }

    int x = 0;
    private boolean ifRight(long start , SessionQueryDataSet sessionQueryDataSet, String path) throws IOException {
        List<List<Object>> ressults = sessionQueryDataSet.getValues();
        File file = new File(path);
        long beg = start;

        for(List<Object> res : ressults) {
            int len = 0;
            int realLen = -1;
            byte[] buffer = null;
            byte[] resByte = (byte[]) res.get(0);
            if (res.get(0) instanceof byte[]) {
                len = resByte.length;
            }
            buffer = getByteFromFile(file,beg,len);
            realLen = buffer.length;
            if (len != realLen) {
//                System.out.println("term " + x + " read at " + beg + " len diff with Len " + len + " and it should be " + realLen);
//                return false;
            }
            long poss = find(resByte, file);
            if (poss!=223 && poss/1024/1024!=x|| poss==-1 ) {
//                return false;
            }
//            for (int i = 0; i < buffer.length; i++) {
//                if (buffer[i] != resByte[i]) {
//                    long whyindex= findInList(ressults,buffer);
//                    System.out.println("term " + x + " read at " + beg + " content diff with " + resByte[i] + " and it should be " + buffer[i] + " at index " + i);
//                    long poss = find(resByte, file);
//                    long poss2 = find(buffer, file);
//                    System.out.println("actual pos " + poss / 1024 / 1024);
//                    return false;
//                }
//            }
            beg += len;
            x++;
        }
        return true;
    }

    private byte[] getByteFromFile(File file, long beg, int len){
        FileInputStream fis = null;
        byte[] data = new byte[len];

        try {
            fis = new FileInputStream(file);
            fis.skip(beg);
            fis.read(data);
        } catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(fis != null){
                    fis.close();
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        return data;
    }

    private int findInList(List<List<Object>> ressults, byte[] r ) {
        int index = 0;
        int realLen = -1;
        byte[] resByte;
        boolean flag = false;
        for(List<Object> res : ressults) {
            index++;
            resByte = (byte[]) res.get(0);
            for (int i = 0; i < r.length; i++) {
                if (r[i] != resByte[i]) {
                    flag = true;
                    break;
                }
                if (!flag) return index;
            }
        }
        return -1;
    }

    public static long find(byte[] pattern, File file) throws IOException {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            byte[] buffer = new byte[pattern.length];
            if(buffer.length!=1024*1024) return 223;
            int read;
            long pos = 0;
            while ((read = inputStream.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    if (buffer[i] == pattern[0]) {
                        boolean match = true;
                        for (int j = 1; j < pattern.length; j++) {
                            if (i+j >= read) {
                                // not enough data left in buffer, read more
                                match = false;
                                break;
                            }
                            if (buffer[i+j] != pattern[j]) {
                                match = false;
                                break;
                            }
                        }
                        if (match) {
                            return pos + i;
                        }
                    }
                }
                pos += read;
            }
            return -1; // pattern not found
        }
    }

    private int BUFFERSIZE = 1024 * 1024*10;

    public class MyFileReader implements Runnable {
        private List<byte[]> res;
        private int index;
        private RandomAccessFile raf;
        private long readPos;

        public MyFileReader(RandomAccessFile raf, long readPos, List<byte[]> res, int index) {
            this.raf=raf;
            this.readPos = readPos;
            this.res = res;
            this.index = index;
        }

        @Override
        public void run() {
            int batchSize = BUFFERSIZE;
            byte[] buffer = new byte[batchSize]; // 一次读取1MB
            int len = 0;
            // Move the file pointer to the starting position
            try {
                raf.seek(readPos);
                // Read the specified range of bytes from the file
                len = raf.read(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (len != batchSize) {
                byte[] subBuffer = new byte[len];
                subBuffer = Arrays.copyOf(buffer, len);
                res.set(index,subBuffer);
            } else res.set(index,buffer);
        }
    }

    @Test
    public void testBasicRead() throws IOException, InterruptedException {
        String fileName = "100GB";
        File file = new File("D:\\LAB\\my\\IGinX\\dataSources\\filesystem\\src\\test\\java\\cn\\edu\\tsinghua\\iginx\\tmp\\a\\"+fileName);

        long startTime = System.currentTimeMillis();
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<byte[]> res = new ArrayList<>();
        for( int i=0;i<1024*100;i++)
            res.add(new byte[0]);

        for(int i=0;i<1024*10;i++){
//            System.gc();

            RandomAccessFile raf = new RandomAccessFile(file, "r");

            // Move the file pointer to the starting position
            raf.seek(i*1024);
            // Read the specified range of bytes from the file
            int finalI = i;
//            executorService.execute(() -> {
            try {
                long startTime1 = System.currentTimeMillis();
                byte[] buffer = new byte[1024 * 1024]; // 一次读取1MB
                raf.read(buffer);
                res.set(finalI%50,buffer);

                long endTime1 = System.currentTimeMillis();
                long totalTimeSeconds1 = (endTime1 - startTime1);
//                    System.out.println("Every thread Function took " + totalTimeSeconds1 + " Millis.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
//            });
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }

        long endTime = System.currentTimeMillis();
        long totalTimeSeconds = (endTime - startTime);
        System.out.println("Function took " + totalTimeSeconds + " Millis.");
    }

    @Test
    public void testHashMap() {
        Map<String,String> map = new HashMap<>();
        map.put("123","123");
        map.put("12343","123");
        String s = new String(JsonUtils.toJson(map));
        Map<String,String> map2 = JsonUtils.fromJson(s.getBytes(),Map.class);
        map2.size();
    }

//    private boolean ifRight(long start , SessionQueryDataSet sessionQueryDataSet, String path) throws IOException {
//        List<List<Object>> ressults = sessionQueryDataSet.getValues();
//        File file = new File(path);
//        long beg = start;
//
//        int x = 0;
//        for(List<Object> res : ressults) {
//            int len = 0;
//            int realLen = -1;
//            byte[] buffer = null;
//            byte[] resByte = (byte[]) res.get(0);
//            if (res.get(0) instanceof byte[]) {
//                len = resByte.length;
//            }
//            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
//                buffer = new byte[len]; // 一次读取1MB
//                raf.seek(beg);
//                realLen = raf.read(buffer);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            if (len != realLen) {
//                System.out.println("term " + x + " read at " + beg + " len diff with Len " + len + " and it should be " + realLen);
//                return false;
//            }
//            for (int i = 0; i < buffer.length; i++) {
//                if (buffer[i] != resByte[i]) {
//                    System.out.println("term " + x + " read at " + beg + " content diff with " + resByte[i] + " and it should be " + buffer[i] + " at index " + i);
//                    long poss = find(resByte, file);
//                    System.out.println("actual pos " + poss / 1024 / 1024);
//                    return false;
//                }
//            }
//            beg += len;
//            x++;
//
//        }
//        return true;
//    }
}
