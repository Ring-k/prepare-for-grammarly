import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckGrammar {

  static String path = "text.txt";

  static String removedCite(String input, String replace) {
    String pattern = "\\\\cite\\{(.+?)\\}";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  static String removedNotation(String input, String replace) {
    String pattern = "\\$(.+?)\\$";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  static String removeRef(String input, String replace) {
    String pattern = "\\\\ref\\{(.+?)\\}";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  static String removeTextFormat(String input, String replace) {
    String pattern = "\\\\text(.+?)\\{";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString().replace("}", "");
  }

  static String removeBegin(String input, String replace) {
    String pattern = "\\\\begin\\{(.+?)\\}";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  static String removeEnd(String input, String replace) {
    String pattern = "\\\\end\\{(.+?)\\}";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  static String removeItem(String input, String replace) {
    String pattern = "\\\\item";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  static String removeLabel(String input, String replace) {
    String pattern = "\\\\label\\{(.+?)\\}";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replace);
    }
    m.appendTail(sb);
    return sb.toString();
  }

//  textit
//  textbf


  public static void main(String[] args) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    FileWriter writer = new FileWriter("text_after.txt");
    String line;
    while ((line = reader.readLine()) != null) {
      line = removeLabel(line, "");
      line = removeBeginEnd(line, "");
      line = removeItem(line, "");
      line = removedCite(line, "(paper 1)");
      line = removedNotation(line, "C");
      line = removeRef(line, "1");
      line = removeTextFormat(line, "");
      line = line.replace("``", "\"").replace("''", "\"");
      writer.write(line + "\n");
    }
    reader.close();
    writer.close();

  }

  private static String removeBeginEnd(String line, String s) {
    line = removeBegin(line, s);
    line = removeEnd(line, s);
    return line;
  }

}
