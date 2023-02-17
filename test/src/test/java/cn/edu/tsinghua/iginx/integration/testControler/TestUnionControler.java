package cn.edu.tsinghua.iginx.integration.testControler;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.TagIT;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class TestUnionControler {
    private final List<String[]> STORAGEENGINELIST = new ArrayList<String[]>(){{
        add(new String[] {"127.0.0.1", "6668", "iotdb12", "username:root, password:root, sessionPoolSize:20, has_data:false, is_read_only:false"});
//        add(new String[] {"127.0.0.1", "8060", "influxdb", "url:http://localhost:8086/ , username:user, password:12345678, sessionPoolSize:20, has_data:true, is_read_only:false, token:testToken, organization:testOrg"});
//        add(new String[] {"127.0.0.1", "6667", "iotdb12", "username:root, password:root, sessionPoolSize:20, has_data:false, is_read_only:false"});
    }};

    // write test IT name to the file
    private void setTestTasks(String DBName) {
        try {
            File file = new File(FILEPATH);//文件路径
            FileWriter fileWriter = new FileWriter(file);
            List<String> taskList = new ArrayList<>();
            // test for specific DB
            switch (DBName.toLowerCase()) {
                case "iotdb12":
                    taskList.addAll(Arrays.asList(
//                            "IoTDB12SessionIT\n",
//                            "IoTDB12SessionPoolIT\n",
                            "IoTDBSQLSessionIT\n",
                            "IoTDBSQLSessionPoolIT\n"
                    ));
                    break;
                case "influxdb":
                    taskList.addAll(Arrays.asList(
                            "InfluxDBSessionIT\n",
                            "InfluxDBSessionPoolIT\n",
                            "InfluxDBSQLSessionIT\n",
                            "InfluxDBSQLSessionPoolIT\n"
                    ));
                    break;
                case "parquet":
                    taskList.addAll(Arrays.asList(
                            "ParquetSQLSessionIT\n",
                            "ParquetSQLSessionPoolIT\n"
                    ));
                    break;
                case "timescaledb":
                    taskList.addAll(Arrays.asList(
                            "TimescaleDBSessionIT\n",
                            "TimescaleDBSessionPoolIT\n"
                    ));
                    break;
                default:
                    break;
            }
            // normal test
            taskList.addAll(Arrays.asList(
                    "TagIT\n",
                    "RestAnnotationIT\n",
                    "RestIT\n"
            ));
            for (String taskName : taskList) {
                fileWriter.write(taskName);
            }
            fileWriter.flush();//刷新数据，不刷新写入不进去
            fileWriter.close();//关闭流
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(TagIT.class);
    private boolean DEBUG = true;
    private String FILEPATH = "./src/test/java/cn/edu/tsinghua/iginx/integration/testControler/testTask.txt";
    public static String CLEARDATAEXCP = "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";
    private String MVNRUNTEST = "../.github/testUnion.sh";
    private String ADDSTORAGEENGINE = "ADD STORAGEENGINE (\"%s\", %s, \"%s\", \"%s\")";
//    private List<String> specialTaskName = Arrays.asList("SessionIT", "SessionPoolIT", "SQLSessionIT", "SQLSessionPoolIT");
//    private List<String> normalTaskName = Arrays.asList("TagIT", "RestIT", "RestAnnotationIT", "TransformIT", "UDFIT");
    protected static Session session;

    private String toCmd(String[] storageEngine) {
        return String.format(ADDSTORAGEENGINE, storageEngine[0], storageEngine[1], storageEngine[2], storageEngine[3]);
    }

    public static void clearData(Session session) throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(clearData);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}", clearData, e.toString());
            if (e.toString().equals(CLEARDATAEXCP) || e.toString().equals("\n" + CLEARDATAEXCP)) {
                logger.error("clear data fail and go on....");
            }
            else fail();
        }

        if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}.", clearData, res.getParseErrorMsg());
            fail();
        }
    }

    public void runShellCommand(String command) throws Exception {
        Process p = null;
        try {
//            p = Runtime.getRuntime().exec(new String[] {"bash", command});
//            InputStream in = null;
//            if (DEBUG)
//                in = p.getErrorStream();
//            else
//                in = p.getInputStream();

            ProcessBuilder builder = new ProcessBuilder("/bin/bash " + command);
            builder.redirectErrorStream(true);
            p = builder.start();
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while((line = br.readLine())!=null){
                System.out.println(line);
            }

//            BufferedReader read = new BufferedReader(new InputStreamReader(in));
//            String line;
//            while((line = read.readLine())!=null){
//                System.out.println(line);
//            }
            int status = p.waitFor();
            System.err.printf("runShellCommand: %s, status: %s%n, %s%n", command, p.exitValue(), status);
            if (p.exitValue() != 0) {
                throw new Exception("tests fail!");
            }
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }

    @BeforeClass
    public static void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
        try {
            session.openSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void testUnion() throws Exception {
        for (String[] cmd :STORAGEENGINELIST) {
            //set the test Environment
            session.executeSql(toCmd(cmd));
            //set the test tasks with DBName
            setTestTasks(cmd[2]);
            //run the test
            runShellCommand(MVNRUNTEST);
        }
    }
}







































