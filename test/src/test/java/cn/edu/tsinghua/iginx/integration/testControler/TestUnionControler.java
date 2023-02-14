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

import java.io.IOException;
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
    private static String CLEARDATAEXCP = "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";
    private String MVNRUNTEST = "mvn test -q -Dtest=%s -DfailIfNoTests=false";
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

    public static void runShellCommand(String command) {
        String[] cmdStrings = new String[] {command};

        Process p = null;
        try {
            p = Runtime.getRuntime().exec(cmdStrings);
            int status = p.waitFor();
            if (status != 0) {
                System.err.printf("runShellCommand: %s, status: %s%n", command, status);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
    public void testUnion() throws SessionException, ExecutionException {
        for (String cmd :STORAGEENGINELIST) {
            //set the test Environment
            session.executeSql(cmd);
            //run the test
            for (String testName : testTasks) {
                runShellCommand(String.format(MVNRUNTEST, testName));
            }
        }
    }
}