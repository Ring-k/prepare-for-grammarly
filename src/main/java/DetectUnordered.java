//import org.apache.iotdb.rpc.IoTDBConnectionException;
//import org.apache.iotdb.rpc.StatementExecutionException;
//import org.apache.iotdb.session.Session;
//import org.apache.iotdb.session.SessionDataSet;
//import org.apache.iotdb.tsfile.read.common.RowRecord;
//
//import java.io.FileWriter;
//import java.io.IOException;
//
//public class DetectUnordered {
//
//  private static Session session;
//  private static Session sessionEnableRedirect;
//
//  public static void main(String[] args)
//      throws IoTDBConnectionException, IOException, StatementExecutionException {
//    String hostIP = "192.168.88.137";
//    int hostPort = 6667;
//
//    FileWriter fw = new FileWriter("OutOfOrderTimeSeries_k.txt");
//
//    session = new Session(hostIP, hostPort, "root", "root");
//    session.open(false);
//
//    // set session fetchSize
//    session.setFetchSize(10000);
//
//    // search all time series name
//    String searchTimeseriesSQL = "show timeseries";
//
//    SessionDataSet ts_dataSet = session.executeQueryStatement(searchTimeseriesSQL);
//
//    while (ts_dataSet.hasNext()) {
//      RowRecord r = ts_dataSet.next();
//      String ts = r.getFields().get(0).getStringValue();
//      String[] paths = ts.split("\\.");
//      String sensor = paths[paths.length - 1];
//      String device = ts.replace("." + sensor, "");
//      String queryRcvTimeSQL =
//          "select " + "work_status_recv_time, " + sensor + " from " + device + " without null any";
//
//      // a new session
//      Session queryDataSession = new Session(hostIP, hostPort, "root", "root");
//      queryDataSession.open(false);
//      SessionDataSet timeDataSet = queryDataSession.executeQueryStatement(queryRcvTimeSQL);
//      long counter = 0;
//      long curMax = -10000;
//      while (timeDataSet.hasNext()) {
//        RowRecord r_ = timeDataSet.next();
//        long recvTime = r_.getFields().get(0).getLongV();
//        if (curMax > recvTime) {
//          String countSQL = "select count" + "(" + sensor + ") from " + device;
//
//          Session countSession = new Session(hostIP, hostPort, "root", "root");
//
//          fw.write(ts + "\n");
//          fw.flush();
//          break;
//        } else {
//          curMax = recvTime;
//        }
//        if (counter++ >= 100000) {
//          break;
//        }
//      }
//      timeDataSet.closeOperationHandle();
//    }
//    ts_dataSet.closeOperationHandle();
//    fw.close();
//  }
//}
