package cn.edu.tsinghua.iginx.integration.testControler;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.TagIT;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class TestUnionControler {
    private List<String> testTasks = new ArrayList<>(Arrays.asList(
            "TagIT",
            "RestIT"
    ));

    protected static final Logger logger = LoggerFactory.getLogger(TagIT.class);

    private final boolean DEBUG = false;
    private static String CLEARDATAEXCP = "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";
    private String MVNRUNTEST = "/home/runner/work/IGinX/IGinX/.github/testUnion.sh";
    protected static Session session;

    private List<String> STORAGEENGINELIST = new ArrayList<>(Arrays.asList(
            "ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"iotdb12\", \"username:root, password:root, sessionPoolSize:20, has_data:false, is_read_only:false\");"
    ));

    public void addTestTasks(String testName) {
        testTasks.add(testName);
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

    public static void runShellCommand(String command) throws Exception {
        String[] cmdStrings = new String[] {"chmod", "+x", command};

        Process p = null;
        try {
            p = Runtime.getRuntime().exec(new String[] {command});
            InputStream in = p.getInputStream(), errorIn = p.getErrorStream();
            BufferedReader read = new BufferedReader(new InputStreamReader(in));
            String line, lastLine;
            while((line = read.readLine())!=null){
                System.out.println(line);
                lastLine = line;
            }
            System.out.println("======================" + line);
            int status = p.waitFor();
            System.err.printf("runShellCommand: %s, status: %s%n, %s%n", command, p.exitValue(), status);
//            if (errorIn.available() != 0) {
//                BufferedReader readError = new BufferedReader(new InputStreamReader(errorIn));
//                String lineError;
//                while((lineError = readError.readLine())!=null){
//                    System.out.printf("error=======================");
//                    System.out.println(lineError);
//                }
//                throw new Exception("tests fail!");
//            }
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
        for (String cmd :STORAGEENGINELIST) {
            //set the test Environment
            session.executeSql(cmd);
            //run the test
            runShellCommand(MVNRUNTEST);
        }
    }
}