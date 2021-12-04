package experiments;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang.RandomStringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class WritingAmplification {

  public static void main(String[] args)
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    String dataFilePathStr = args[0].trim();
    String ip = "127.0.0.1";
    int port = 6667;
    int stringLength = 16;
    String deviceId = "root.sg1.d1";
    String sensorID = "s1";

    if (args.length == 2) {
      ip = args[1].trim();
    } else if (args.length == 3) {
      ip = args[1].trim();
      stringLength = Integer.parseInt(args[2]);
    }
    Session session = new Session(ip, port, "root", "root");
    session.open(false);
    BufferedReader reader = new BufferedReader(new FileReader(dataFilePathStr));
    String line;
    long amount = 0;
    long duration = 0;
    while ((line = reader.readLine()) != null) {
      String[] sep = line.split(",");
      long genTime = Long.parseLong(sep[0]);
      String value = RandomStringUtils.randomAlphanumeric(stringLength);
      long start = System.nanoTime();
      session.insertRecord(
          deviceId,
          genTime,
          Collections.singletonList(sensorID),
          Collections.singletonList(TSDataType.TEXT),
          Collections.singletonList(value));
      duration += (System.nanoTime() - start);
      amount++;
    }
    System.out.println(
        "insert pts:\t" + amount + "\t duration:\t" + duration + " ns\t" + "thoughput(ns)\t" + (
            (float) amount / duration) + "\tthoughput(ms)\t" + ((float) amount / (duration
            / 1000000)));
    session.close();
  }
}
