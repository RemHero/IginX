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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class TestUnionControler {
//    private String iginxName = new Config().getIginxName();
//    private List<String> testTasks = new ArrayList<>(Arrays.asList(
//            "TagIT",
//            "RestIT"
//    ));
    private final List<String[]> STORAGEENGINELIST = new ArrayList<String[]>(){{
        add(new String[] {"\"127.0.0.1\"", "6668", "\"iotdb12\"", "\"username:root, password:root, sessionPoolSize:20, has_data:false, is_read_only:false\""});
    }};

    protected static final Logger logger = LoggerFactory.getLogger(TagIT.class);
    private static String CLEARDATAEXCP = "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";
    private String MVNRUNTEST = "/home/runner/work/IGinX/IGinX/.github/testUnion.sh";
    private String ADDSTORAGEENGINE = "ADD STORAGEENGINE (%s, %s, %s, %s)";
    private List<String> specialTaskName = Arrays.asList("SessionIT", "SessionPoolIT", "SQLSessionIT", "SQLSessionPoolIT");
    private List<String> normalTaskName = Arrays.asList("TagIT", "RestIT", "RestAnnotationIT", "TransformIT", "UDFIT");
    protected static Session session;

    private String toCMD(String[] storageEngine) {
        return String.format(ADDSTORAGEENGINE, storageEngine[0], storageEngine[1], storageEngine[2], storageEngine[3]);
    }

    public static void clearData(Session session) throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(clearData);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", clearData, e);
            if (e.toString().equals(CLEARDATAEXCP)) {
                logger.error("clear data fail and go on....");
            }
            else fail();
        }

        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}.", clearData, res.getParseErrorMsg());
            fail();
        }
    }

    public void runShellCommand(String command) throws Exception {
//        String[] cmdStrings = new String[] {"chmod", "+x", command};

        Process p = null;
        try {
            p = Runtime.getRuntime().exec(new String[] {command});
            InputStream in = p.getInputStream();
            BufferedReader read = new BufferedReader(new InputStreamReader(in));
            String line;
            while((line = read.readLine())!=null){
                System.out.println(line);
            }
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

    // write test IT name to the file
    private void setTestTasks(String DBName) {
        try {
            File file = new File("./src/test/java/cn/edu/tsinghua/iginx/integration/testControler/testTask.txt");//文件路径
            FileWriter fileWriter = new FileWriter(file);
            if (DBName.contains("IoTDB")) {
                fileWriter.write("IoTDB12SessionIT\n");
                fileWriter.write("IoTDB12SessionPoolIT\n");
                fileWriter.write("IoTDBSQLSessionIT\n");
                fileWriter.write("IoTDBSQLSessionPoolIT\n");
            }
            if (DBName.contains("InfluxDB")) {
                fileWriter.write("InfluxDBSessionIT\n");
                fileWriter.write("InfluxDBSessionPoolIT\n");
                fileWriter.write("InfluxDBSQLSessionIT\n");
                fileWriter.write("InfluxDBSQLSessionPoolIT\n");
            }
            fileWriter.write("TagIT\n");
            fileWriter.write("RestIT\n");
//            fileWriter.write("RestAnnotationIT\n");

            fileWriter.flush();//刷新数据，不刷新写入不进去
            fileWriter.close();//关闭流
        } catch (Exception e) {
            e.printStackTrace();
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
//        for (String[] cmd :STORAGEENGINELIST) {
            //set the test Environment
            session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"iotdb12\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            //set the test tasks with DBName
//            setTestTasks(cmd[2]);
            //run the test
            runShellCommand(MVNRUNTEST);
//        }
    }
}








































