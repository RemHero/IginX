/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import java.util.Map;

public class QueryAggregatorSampler extends QueryAggregator {
  public QueryAggregatorSampler() {
    super(QueryAggregatorType.SAMPLER);
  }

  @Override
  public QueryResultDataset doAggregate(
      RestSession session,
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey) {
    QueryResultDataset queryResultDataset = new QueryResultDataset();
      SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startKey, endKey, tagList);
      queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
      DataType type = RestUtils.checkType(sessionQueryDataSet);
      int n = sessionQueryDataSet.getKeys().length;
      int m = sessionQueryDataSet.getPaths().size();
      int datapoints = 0;
      switch (type) {
          // 当前数据点的值 * （单位时间(unit) / （当前点的时间戳 - 前一个点的时间戳））

        case LONG:
        case DOUBLE:
          Double nowd = null;
          for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
              if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                if (nowd == null) {
                  nowd = (double) sessionQueryDataSet.getValues().get(i).get(j);
                }
                datapoints += 1;
              }
            }
            if (i != 0) {
              queryResultDataset.add(
                  sessionQueryDataSet.getKeys()[i],
                  nowd
                      * getUnit()
                      / (sessionQueryDataSet.getKeys()[i] - sessionQueryDataSet.getKeys()[i - 1]));
            }
            nowd = null;
          }
          queryResultDataset.setSampleSize(datapoints);
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type");
      }
    return queryResultDataset;
  }
}
