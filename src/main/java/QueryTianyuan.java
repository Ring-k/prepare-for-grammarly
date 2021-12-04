import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class QueryTianyuan {

  private static Session session;
  private static Session sessionEnableRedirect;

//  static String timeseriesPath = "root.kobelco.trans.00.1090001200.2401260.KOB_0002_00_134";

  static String ip = "192.168.35.162";
  static String timeseriesPath = "root.kobelco.trans.39.1090001089.2401145.J_0001_00_3673";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, IOException {

    System.out.println("path:" + timeseriesPath);
    String[] sep = timeseriesPath.split("\\.");
    String sensor = sep[sep.length - 1].trim();

    String[] deviceSep = new String[sep.length - 1];
    System.arraycopy(sep, 0, deviceSep, 0, sep.length - 1);
    String device = String.join(".", deviceSep);
//
    session = new Session(ip, 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    File f =
        new File("C:\\Users\\yuyua\\Desktop\\git\\ICDE-2022\\data\\tianyuan\\" + timeseriesPath
            + ".work_status_recv_time.csv");
    FileWriter fw = new FileWriter(f);
    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select work_status_recv_time, " + sensor
                + " from " + device + " without null any");
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      long genTime = r.getTimestamp();
      long arrTime = r.getFields().get(0).getLongV();
      fw.write(genTime + "," + arrTime + "," + (arrTime - genTime) + "\n");
      //      lines = lines.replace('\t', ',');
      //      fw.write(lines + "\n");
    }
    fw.close();
    dataSet.closeOperationHandle();
  }
}
