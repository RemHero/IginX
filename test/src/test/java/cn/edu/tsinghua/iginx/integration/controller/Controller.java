package cn.edu.tsinghua.iginx.integration.controller;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.expPort;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.oriPort;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.parquet;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.parquet.ParquetHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {

  private static final Logger logger = LoggerFactory.getLogger(Controller.class);

  public static final String CLEAR_DATA = "CLEAR DATA;";

  public static final String CLEAR_DATA_WARNING = "clear data fail and go on...";

  public static final String CLEAR_DATA_ERROR = "Statement: \"{}\" execute fail. Caused by: {}";

  public static final String CONFIG_FILE = "./src/test/resources/testConfig.properties";

  private static final String TEST_TASK_FILE = "./src/test/resources/testTask.txt";

  private static final String MVN_RUN_TEST = "../.github/scripts/test/test_union.sh";

  private static final String ADD_STORAGE_ENGINE_PARQUET =
      "ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"parquet\", \"has_data:true, is_read_only:true, dir:test/parquet, dummy_dir:%s, iginx_port:6888, data_prefix:%s\");";

  // 将数据划分为两部分，一部分写入dummy数据库，一部分写入非dummy数据库, 0.3 为划分比例，即 30% 的数据写入 dummy 数据库
  private static final double PARTITION_POINT = 0.3;
  // 向 dummy 分片写入的初始化序列，用来初始化 dummy 分片的原数据空间范围
  public static final String DUMMY_INIT_PATH_BEGIN = "b.b.b";
  public static final String DUMMY_INIT_PATH_END =
      "zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
  private static final String EXP_HAS_DATA_STRING = "ExpHasData";
  private static final String ORI_HAS_DATA_STRING = "OriHasData";
  private static final ConfLoader testConf = new ConfLoader(Controller.CONFIG_FILE);

  public static final Map<String, Boolean> SUPPORT_KEY =
      new HashMap<String, Boolean>() {
        {
          put(
              testConf.getStorageType(),
              testConf
                  .loadDBConf(testConf.getStorageType())
                  .getEnumValue(DBConf.DBConfType.isSupportKey));
        }
      };

  public static final Map<String, Boolean> NEED_SEPARATE_WRITING =
      new HashMap<String, Boolean>() {
        {
          put(
              testConf.getStorageType(),
              testConf
                  .loadDBConf(testConf.getStorageType())
                  .getEnumValue(DBConf.DBConfType.isSupportDiffTypeHistoryData));
        }
      };

  public static final Boolean IS_EXP_DUMMY =
      testConf.getDBCETestWay().contains(EXP_HAS_DATA_STRING);
  public static final Boolean IS_ORI_DUMMY =
      testConf.getDBCETestWay().contains(ORI_HAS_DATA_STRING);

  public static void clearAllData(Session session) {
    clearAllData(new MultiConnection(session));
  }

  public static void clearAllData(MultiConnection session) {
    clearData(session);
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (!conf.isScaling()) {
      logger.info("Not the DBCE test, skip the clear history data step.");
    } else {
      BaseHistoryDataGenerator generator = getCurrentGenerator(conf);
      if (generator == null) {
        logger.error("clear data fail, caused by generator is null");
        return;
      }
      generator.clearHistoryData();
    }
  }

  public static void clearData(Session session) {
    clearData(new MultiConnection(session));
  }

  public static void clearData(MultiConnection session) {
    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(CLEAR_DATA);
    } catch (SessionException | ExecutionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
        logger.warn(CLEAR_DATA_WARNING);
      } else {
        logger.error(CLEAR_DATA_ERROR, CLEAR_DATA, e.getMessage());
        fail();
      }
    }

    if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error(CLEAR_DATA_ERROR, CLEAR_DATA, res.getParseErrorMsg());
      fail();
    }
  }

  private static BaseHistoryDataGenerator getCurrentGenerator(ConfLoader conf) {
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    String instance = dbConf.getHistoryDataGenClassName();
    try {
      return (BaseHistoryDataGenerator) Class.forName(instance).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      logger.error("write data fail, caused by: {}", e.getMessage());
      fail();
    }
    return null;
  }

  public static <T> void writeColumnsData(
      T session,
      List<String> pathList,
      List<List<Long>> keyList,
      List<DataType> dataTypeList,
      List<List<Object>> valuesList,
      List<Map<String, String>> tagsList,
      InsertAPIType type,
      boolean needWriteHistoryData) {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    // medium 为划分数据的分界点，即前 medium 个数据写入非 dummy 数据库，后 medium 个数据写入 dummy 数据库
    int medium = 0;
    if (!conf.isScaling() || !NEED_SEPARATE_WRITING.get(conf.getStorageType())) {
      logger.info("skip the write history data step.");
      medium = pathList.size();
    } else {
      logger.info("DBCE test, write history data.");
      medium =
          (tagsList == null
                      || tagsList.isEmpty()
                      || tagsList.stream().allMatch(map -> map.size() == 0))
                  && (IS_EXP_DUMMY || IS_ORI_DUMMY)
              ? (int) (pathList.size() * PARTITION_POINT)
              : pathList.size();
    }
    logger.info("show the medium: {}", medium);

    for (int i = 0; i < pathList.size(); i++) {
      if (i <= medium) {
        try { // write data through session
          Object[] value = valuesList.get(i).toArray();
          Object[] valueToInsert = new Object[1];
          valueToInsert[0] = value;
          writeDataWithSession(
              session,
              Collections.singletonList(pathList.get(i)),
              keyList.get(i).stream().mapToLong(Long::longValue).toArray(),
              valueToInsert,
              Collections.singletonList(dataTypeList.get(i)),
              Collections.singletonList(tagsList.get(i)),
              type);
        } catch (SessionException | ExecutionException e) {
          logger.error("write data fail, caused by: {}", e.getMessage());
          fail();
        }
      } else {
        if (!needWriteHistoryData) {
          break;
        }
        logger.info("wrong step!");
        int port = IS_EXP_DUMMY ? expPort : oriPort;
        List<List<Object>> rowValues = convertColumnsToRows(valuesList.get(i));
        BaseHistoryDataGenerator generator = getCurrentGenerator(conf);
        if (generator == null) {
          logger.error("write data fail, caused by generator is null");
          return;
        }
        if (StorageEngineType.valueOf(conf.getStorageType().toLowerCase()) == parquet) {
          ParquetHistoryDataGenerator parquetGenerator = (ParquetHistoryDataGenerator) generator;
          String path = pathList.get(i);
          String tableName = path.substring(0, path.indexOf("."));
          String dir =
              ParquetHistoryDataGenerator.IT_DATA_DIR
                  + System.getProperty("file.separator")
                  + tableName;
          parquetGenerator.writeHistoryData(
              port,
              dir,
              ParquetHistoryDataGenerator.IT_DATA_FILENAME,
              Collections.singletonList(pathList.get(i)),
              Collections.singletonList(dataTypeList.get(i)),
              keyList.get(i),
              rowValues);
          try {
            addEmbeddedStorageEngine(
                session, String.format(ADD_STORAGE_ENGINE_PARQUET, dir, tableName));
          } catch (SessionException | ExecutionException e) {
            if (!e.getMessage().contains("unexpected repeated add")) {
              logger.error("add embedded storage engine fail, caused by: {}", e.getMessage());
              fail();
            }
          }
        } else {
          generator.writeHistoryData(
              port,
              Collections.singletonList(pathList.get(i)),
              Collections.singletonList(dataTypeList.get(i)),
              keyList.get(i),
              rowValues);
        }
      }
    }
  }

  public static <T> void writeRowsData(
      T session,
      List<String> pathList,
      List<Long> keyList,
      List<DataType> dataTypeList,
      List<List<Object>> valuesList,
      List<Map<String, String>> tagsList,
      InsertAPIType type,
      boolean needWriteHistoryData) {
    logger.info("write rows data" + pathList);
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    // medium 为划分数据的分界点，即前 medium 个数据写入非 dummy 数据库，后 medium 个数据写入 dummy 数据库
    int medium = 0;
    if (!conf.isScaling() || !NEED_SEPARATE_WRITING.get(conf.getStorageType())) {
      logger.info("skip the write history data step.");
      medium = keyList.size();
    } else {
      logger.info("DBCE test, write history data.");
      medium =
          (tagsList == null
                      || tagsList.isEmpty()
                      || tagsList.stream().allMatch(map -> map.size() == 0))
                  && (IS_EXP_DUMMY || IS_ORI_DUMMY)
              ? (int) (keyList.size() * PARTITION_POINT)
              : keyList.size();
    }
    logger.info("show the medium: {}", medium);

    // divide the data
    List<Long> upperkeyList = keyList;
    List<Long> lowerKeyList = null;
    List<List<Object>> upperValuesList = valuesList;
    List<List<Object>> lowerValuesList = null;
    // 划分数据区间
    if (medium != keyList.size() && medium != 0) {
      logger.info("wrong step!");
      upperkeyList = keyList.subList(0, medium); // 上半部分，包括索引为 0 到 midIndex-1 的元素
      lowerKeyList =
          keyList.subList(medium, keyList.size()); // 下半部分，包括索引为 midIndex 到 keyList.size()-1 的元素
      upperValuesList = valuesList.subList(0, medium);
      lowerValuesList = valuesList.subList(medium, keyList.size());
    }

    // transfer the List to Array
    Object[] newValuesList = new Object[upperkeyList.size()];
    for (int j = 0; j < upperkeyList.size(); j++) {
      Object[] value = new Object[pathList.size()];
      for (int k = 0; k < pathList.size(); k++) {
        value[k] = upperValuesList.get(j).get(k);
      }
      newValuesList[j] = value;
    }

    try { // write data through session
      logger.info("write data through session");
      writeDataWithSession(
          session,
          pathList,
          upperkeyList.stream().mapToLong(Long::longValue).toArray(),
          newValuesList,
          dataTypeList,
          tagsList,
          type);
    } catch (SessionException | ExecutionException e) {
      logger.error("write data fail, caused by: {}", e.getMessage());
      fail();
    }

    if (lowerKeyList != null && !lowerKeyList.isEmpty() && needWriteHistoryData) {
      logger.info("write data through history data generator");
      int port = IS_EXP_DUMMY ? expPort : oriPort;
      BaseHistoryDataGenerator generator = getCurrentGenerator(conf);
      if (generator == null) {
        logger.error("write data fail, caused by generator is null");
        return;
      }
      if (StorageEngineType.valueOf(conf.getStorageType().toLowerCase()) == parquet) {
        ParquetHistoryDataGenerator parquetGenerator = (ParquetHistoryDataGenerator) generator;
        String path = pathList.get(0);
        String tableName = path.substring(0, path.indexOf("."));
        String dir =
            ParquetHistoryDataGenerator.IT_DATA_DIR
                + System.getProperty("file.separator")
                + tableName;
        parquetGenerator.writeHistoryData(
            port,
            dir,
            ParquetHistoryDataGenerator.IT_DATA_FILENAME,
            pathList,
            dataTypeList,
            lowerKeyList,
            lowerValuesList);
        try {
          addEmbeddedStorageEngine(
              session, String.format(ADD_STORAGE_ENGINE_PARQUET, dir, tableName));
        } catch (SessionException | ExecutionException e) {
          if (!e.getMessage().contains("repeatedly add storage engine")) {
            logger.error("add embedded storage engine fail, caused by: {}", e.getMessage());
            fail();
          }
        }
      } else {
        generator.writeHistoryData(port, pathList, dataTypeList, lowerKeyList, lowerValuesList);
      }
    }
  }

  private static <T> void addEmbeddedStorageEngine(T session, String stmt)
      throws SessionException, ExecutionException {
    MultiConnection multiConnection = null;
    if (session instanceof MultiConnection) {
      multiConnection = ((MultiConnection) session);
    } else if (session instanceof Session) {
      multiConnection = new MultiConnection(((Session) session));
    }
    multiConnection.executeSql(stmt);
  }

  private static <T> void writeDataWithSession(
      T session,
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      InsertAPIType type)
      throws SessionException, ExecutionException {
    MultiConnection multiConnection = null;
    if (session instanceof MultiConnection) {
      multiConnection = ((MultiConnection) session);
    } else if (session instanceof Session) {
      multiConnection = new MultiConnection(((Session) session));
    }
    switch (type) {
      case Row:
        multiConnection.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
      case NonAlignedRow:
        multiConnection.insertNonAlignedRowRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
      case Column:
        multiConnection.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
      case NonAlignedColumn:
        multiConnection.insertNonAlignedColumnRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
    }
  }

  public static <T> List<List<T>> convertColumnsToRows(List<T> inputList) {
    List<List<T>> outputList = new ArrayList<>();

    for (T item : inputList) {
      List<T> subList = new ArrayList<>();
      subList.add(item);
      outputList.add(subList);
    }

    return outputList;
  }

  @Test
  public void testUnion() throws Exception {
    // load the test conf
    ConfLoader testConfLoader = new ConfLoader(CONFIG_FILE);
    testConfLoader.loadTestConf();

    ShellRunner shellRunner = new ShellRunner();
    TestEnvironmentController envir = new TestEnvironmentController();

    // set the task list
    envir.setTestTasks(
        testConfLoader
            .getTaskMap()
            .get(StorageEngineType.valueOf(testConfLoader.getStorageType().toLowerCase())),
        TEST_TASK_FILE);
    // run the test together
    shellRunner.runShellCommand(MVN_RUN_TEST);
  }
}
