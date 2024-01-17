package cn.edu.tsinghua.iginx.integration.func.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestIT {

  private static final Logger logger = LoggerFactory.getLogger(RestIT.class);

  private boolean isAbleToClearData = true;

  private static Session session;

  private boolean isAbleToDelete = true;

  public static int getLineNumber() {
    // 返回调用 getLineNumber 方法的代码行数
    return Thread.currentThread().getStackTrace()[2].getLineNumber();
  }

  public RestIT() {
    System.out.println("RestIT Begin!");
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    System.out.println(getLineNumber());
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    System.out.println(getLineNumber());
    this.isAbleToClearData = dbConf.getEnumValue(DBConf.DBConfType.isAbleToClearData);
    System.out.println(getLineNumber());
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    System.out.println(getLineNumber());
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    System.out.println(getLineNumber());
    session = new Session("127.0.0.1", 6888, "root", "root");
    System.out.println(getLineNumber());
    session.openSession();
    System.out.println(getLineNumber());
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    System.out.println(getLineNumber());
    session.closeSession();
  }

  @Before
  public void insertData() {
    try {
      System.out.println(getLineNumber());
      execute("insert.json", TYPE.INSERT);
      System.out.println(getLineNumber());
    } catch (Exception e) {
      logger.error("insertData fail. Caused by: {}.", e.toString());
      fail();
    }
  }

  @After
  public void clearData() {
    System.out.println(getLineNumber());
    Controller.clearData(session);
    System.out.println(getLineNumber());
  }

  public enum TYPE {
    QUERY,
    INSERT,
    DELETE,
    DELETE_METRIC
  }

  public String orderGen(String json, TYPE type) {
    System.out.println(getLineNumber());
    StringBuilder ret = new StringBuilder();
    if (type.equals(TYPE.DELETE_METRIC)) {
      ret.append("curl -XDELETE");
      ret.append(" http://127.0.0.1:6666/api/v1/metric/{");
      ret.append(json);
      ret.append("}");
    } else {
      ret.append("curl -XPOST -H\"Content-Type: application/json\" -d @");
      ret.append(json);
      if (type.equals(TYPE.QUERY)) {
        ret.append(" http://127.0.0.1:6666/api/v1/datapoints/query");
      } else if (type.equals(TYPE.INSERT)) {
        ret.append(" http://127.0.0.1:6666/api/v1/datapoints");
      } else if (type.equals(TYPE.DELETE)) {
        ret.append(" http://127.0.0.1:6666/api/v1/datapoints/delete");
      }
    }
    return ret.toString();
  }

  public void printLog() {
    try {
      ProcessBuilder processBuilder = new ProcessBuilder("/bin/bash", "-c", "cat iginx-*.log");
      Process process = processBuilder.start();

      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }

      int exitCode = process.waitFor();
      System.out.println("\nExited with error code : " + exitCode);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public String execute(String json, TYPE type) throws Exception {
    System.out.println(getLineNumber());
    StringBuilder ret = new StringBuilder();
    String curlArray = orderGen(json, type);
    Process process = null;
    try {
      ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
      processBuilder.directory(new File("./src/test/resources/restIT"));
      // 执行 url 命令
      printLog();
      process = processBuilder.start();
      System.out.println(getLineNumber());
      printLog();

      // 输出子进程信息
      InputStreamReader inputStreamReaderINFO = new InputStreamReader(process.getInputStream());
      System.out.println(getLineNumber());
      BufferedReader bufferedReaderINFO = new BufferedReader(inputStreamReaderINFO);
      System.out.println(getLineNumber());
      String lineStr;
      printLog();
      while ((lineStr = bufferedReaderINFO.readLine()) != null) {
        System.out.println(getLineNumber());
        System.out.println(lineStr);
        ret.append(lineStr);
      }
      System.out.println(getLineNumber());
      // 等待子进程结束
      process.waitFor();
      System.out.println(getLineNumber());

      return ret.toString();
    } catch (InterruptedException e) {
      // 强制关闭子进程（如果打开程序，需要额外关闭）
      process.destroyForcibly();
      return null;
    }
  }

  private void executeAndCompare(String json, String output) {
    System.out.println(getLineNumber());
    String result = "";
    try {
      result = execute(json, TYPE.QUERY);
    } catch (Exception e) {
      //            if (e.toString().equals())
      logger.error("executeAndCompare fail. Caused by: {}.", e.toString());
    }
    assertEquals(output, result);
  }

  @Test
  public void testQueryWithoutTags() {
    System.out.println(getLineNumber());
    String json = "testQueryWithoutTags.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]},{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.search\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"host\": [\"server2\"]}, \"values\": [[1359786400000,321.0]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWithTags() {
    System.out.println(getLineNumber());
    String json = "testQueryWithTags.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWrongTags() {
    System.out.println(getLineNumber());
    String json = "testQueryWrongTags.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryOneTagWrong() {
    System.out.println(getLineNumber());
    String json = "testQueryOneTagWrong.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWrongName() {
    System.out.println(getLineNumber());
    String json = "testQueryWrongName.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_.a.b.c\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWrongTime() {
    System.out.println(getLineNumber());
    String json = "testQueryWrongTime.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  //    @Test
  //    public void testQuery(){
  //        String json = "";
  //        String result = "";
  //        executeAndCompare(json,result);
  //    }

  @Test
  public void testQueryAvg() {
    System.out.println(getLineNumber());
    String json = "testQueryAvg.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788298001,13.2],[1359788398001,123.3],[1359788408001,23.1]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryCount() {
    System.out.println(getLineNumber());
    String json = "testQueryCount.json";
    String result =
        "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,3]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryFirst() {
    System.out.println(getLineNumber());
    String json = "testQueryFirst.json";
    String result =
        "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryLast() {
    System.out.println(getLineNumber());
    String json = "testQueryLast.json";
    String result =
        "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,23.1]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryMax() {
    System.out.println(getLineNumber());
    String json = "testQueryMax.json";
    String result =
        "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,123.3]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryMin() {
    System.out.println(getLineNumber());
    String json = "testQueryMin.json";
    String result =
        "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQuerySum() {
    System.out.println(getLineNumber());
    String json = "testQuerySum.json";
    String result =
        "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,159.6]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testDelete() throws Exception {
    System.out.println(getLineNumber());
    if (!isAbleToDelete) {
      return;
    }
    String json = "testDelete.json";
    execute(json, TYPE.DELETE);

    String result =
        "{\"queries\":[{\"sample_size\": 2,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788410000,23.1]]}]}]}";
    json = "testQueryWithTags.json";
    executeAndCompare(json, result);
  }

  @Test
  public void testDeleteMetric() throws Exception {
    System.out.println(getLineNumber());
    if (!isAbleToDelete) {
      return;
    }
    String json = "archive.file.tracked";
    execute(json, TYPE.DELETE_METRIC);

    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    json = "testQueryWithTags.json";
    executeAndCompare(json, result);
  }

  @Test
  @Ignore
  // TODO this test makes no sense
  public void pathValidTest() {
    System.out.println(getLineNumber());
    try {
      String res = execute("pathValidTest.json", TYPE.INSERT);
      logger.warn("insertData fail. Caused by: {}.", res);
    } catch (Exception e) {
      logger.error("insertData fail. Caused by: {}.", e.toString());
    }
  }
}
