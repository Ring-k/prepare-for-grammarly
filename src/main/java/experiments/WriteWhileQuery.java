package experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.chrono.ThaiBuddhistEra;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

// 边写边查
public class WriteWhileQuery {

  public static void main(String[] args)
      throws IoTDBConnectionException, IOException, StatementExecutionException, InterruptedException {
    String dataFilePathStr = args[0].trim();
    final int windowLength = Integer.parseInt(args[1].trim());
    final int sleepInterval = Integer.parseInt(args[2].trim());
    Random random = new Random();
    String ip = "127.0.0.1";
    int port = 6667;
    int stringLength = 16;
    String deviceId = "root.sg1.d1";
    String sensorID = "s1";
    if (args.length == 4) {
      ip = args[3].trim();
    } else if (args.length == 5) {
      ip = args[3].trim();
      stringLength = Integer.parseInt(args[4]);
    }
    final int stringLength_ = stringLength;
    Session writeSession = new Session(ip, port, "root", "root");
    writeSession.open(false);
    Session readSession = new Session(ip, port, "root", "root");
    readSession.open();

    // check if the data writing is finished
    AtomicBoolean writeFinished = new AtomicBoolean(false);

    // record the current maximum write for query
    AtomicLong currentMaxGenTime = new AtomicLong(-1);

    // the writing thread
    Thread writeThread = new Thread(() -> {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(dataFilePathStr));
        String line;
        while ((line = reader.readLine()) != null) {
          String[] sep = line.split(",");
          long genTime = Long.parseLong(sep[0]);
          String value = RandomStringUtils.randomAlphanumeric(stringLength_);
          writeSession.insertRecord(
              deviceId,
              genTime,
              Collections.singletonList(sensorID),
              Collections.singletonList(TSDataType.TEXT),
              Collections.singletonList(value));
          currentMaxGenTime.set(Math.max(genTime, currentMaxGenTime.get()));
        }
//        Thread.sleep(100000);
        writeFinished.set(true);
      } catch (IOException | StatementExecutionException | IoTDBConnectionException e) {
        e.printStackTrace();
      }
    });
    writeThread.start();

    Thread readThread = new Thread(() -> {
      while (!writeFinished.get()) {
        try {
          Thread.sleep(sleepInterval);
          long currentMaxGen = currentMaxGenTime.get();
          if (currentMaxGen < windowLength) {
            continue;
          }
//          long startTime = random.nextInt((int) (currentMaxGen - windowLength));
          long startTime = currentMaxGen - windowLength;
//          long endTime = startTime + windowLength;
          String sql =
              "select * from " + deviceId + " where time > " + startTime ;
          long currentTime = System.nanoTime();
          SessionDataSet dataset = readSession.executeQueryStatement(sql);
          while (dataset.hasNext()) {
            RowRecord rowRecord = dataset.next();
          }
          long duration = System.nanoTime() - currentTime;
          System.out.printf("Executing SQL: %s\t:%dns%n", sql, duration);
        } catch (StatementExecutionException | IoTDBConnectionException | InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    readThread.start();

    writeThread.join();
    readThread.join();

    writeSession.close();
    readSession.close();
  }
}
