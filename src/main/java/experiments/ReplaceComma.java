package experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ReplaceComma {

  public static void main(String[] args) throws IOException {
    String path = "C:\\Users\\yuyua\\Desktop\\git\\ICDE-2022\\data\\my_experiment";
    String fileName = "delay.csv";
    String outName = "after.csv";

    BufferedReader reader = new BufferedReader(new FileReader(path + "\\" + fileName));
    FileWriter writer = new FileWriter(path + "\\" + outName);
    String readLine;
    long counter = 1000000;
    while ((readLine = reader.readLine()) != null && counter --> 0) {
      String writeLine = readLine.replace(", ", "\t");
      writer.write(writeLine + "\n");
    }
    reader.close();
    writer.close();
  }

}
