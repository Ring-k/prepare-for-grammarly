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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;

import java.io.FileWriter;
import java.io.IOException;

public class SessionPoolDetectOutOfOrder {

  private static SessionPool pool;

  // 192.168.35.162
  static String hostIP = "192.168.35.162";
  static int hostPort = 6667;

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException,
          IOException {

//    hostIP = args[0].trim();
    pool = new SessionPool(hostIP, hostPort, "root", "root", 5);

    FileWriter fw = new FileWriter("OutOfOrderTimeSeries.txt");
    String searchTimeseriesSQL = "show timeseries root.kobelco.trans.39";

    SessionDataSetWrapper tsDataSet = pool.executeQueryStatement(searchTimeseriesSQL);

    while (tsDataSet.hasNext()) {
      String ts = tsDataSet.next().getFields().get(0).getStringValue();
      if (ts.contains("work_status_")){
        continue;
      }
      String[] paths = ts.split("\\.");
      String sensor = paths[paths.length - 1];
      String device = ts.replace("." + sensor, "");
      String queryRcvTimeSQL =
          "select " + "work_status_recv_time, " + sensor + " from " + device + " without null any";
      SessionDataSetWrapper timeDataSet;
      try {
        timeDataSet = pool.executeQueryStatement(queryRcvTimeSQL);
      } catch (StatementExecutionException e) {
        System.out.println(e.getMessage());
        fw.write(queryRcvTimeSQL + ", timeout" + "\n");
        fw.flush();
        continue;
      }
      long counter = 0;
      long curMax = -10000;
      while (timeDataSet.hasNext()) {
        long recvTime = timeDataSet.next().getFields().get(0).getLongV();
        if (curMax > recvTime) {
          String countSQL = "select count" + "(" + sensor + ") from " + device;
          SessionDataSetWrapper countTotalDataSet;
          try {
            countTotalDataSet = pool.executeQueryStatement(countSQL);
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            System.out.println(e.getMessage());
            fw.write(countSQL + ", timeout" + "\n");
            fw.flush();
            break;
          }
          fw.write(ts + ',' + countTotalDataSet.next().getFields().get(0).getLongV() + "\n");
          fw.flush();
          pool.closeResultSet(countTotalDataSet);
          break;
        } else {
          curMax = recvTime;
        }
        if (counter++ >= 100000) {
          break;
        }
      }
      pool.closeResultSet(timeDataSet);
    }
    pool.closeResultSet(tsDataSet);
    pool.close();
  }
}
