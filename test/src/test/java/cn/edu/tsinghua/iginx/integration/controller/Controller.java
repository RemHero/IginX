package cn.edu.tsinghua.iginx.integration.controller;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBType;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    public static final String CLEAR_DATA_EXCEPTION =
            "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";

    public static final String CONFIG_FILE = "./src/test/resources/testConfig.properties";

    private static final String TEST_TASK_FILE = "./src/test/resources/testTask.txt";

    private static final String MVN_RUN_TEST = "../.github/test_union.sh";

    private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();

    public static void clearData(Session session) {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(clearData);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}", clearData, e.toString());
            if (e.toString().equals(CLEAR_DATA_EXCEPTION)
                    || e.toString().equals("\n" + CLEAR_DATA_EXCEPTION)) {
                logger.error("clear data fail and go on....");
            } else {
                fail();
            }
        }

        if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error(
                    "Statement: \"{}\" execute fail. Caused by: {}.",
                    clearData,
                    res.getParseErrorMsg());
            fail();
        }
    }

    @Test
    public void testUnion() throws Exception {
        // load the test conf
        ConfLoader testConfLoader = new ConfLoader(CONFIG_FILE);
        testConfLoader.loadTestConf();
        storageEngineMetas = testConfLoader.getStorageEngineMetas();

        ShellRunner shellRunner = new ShellRunner();
        TestEnvironmentController envir = new TestEnvironmentController();

        // ori plan
        //        // skip this when support remove Engine
        //        shellRunner.runShellCommand(MVNRUNTEST);
        //        // for each storage , run the test
        //        for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
        //            // add the storage engine
        //            envir.addStorageEngine(storageEngineMeta);
        //            // set the task list
        //
        // envir.setTestTasks(testConfLoader.getTaskMap().get(storageEngineMeta.getStorageEngine()),
        // FILEPATH);
        //            // run the test together
        //            shellRunner.runShellCommand(MVNRUNTEST);
        //        }

        // set the task list
        envir.setTestTasks(
                testConfLoader
                        .getTaskMap()
                        .get(DBType.valueOf(testConfLoader.getStorageType().toLowerCase())),
                TEST_TASK_FILE);
        // run the test together
        shellRunner.runShellCommand(MVN_RUN_TEST);
    }
}
