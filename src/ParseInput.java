/**
 * Reads the input and parses it
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
public class ParseInput {

  private static int time;

  /**
   * Parses a single line from the text file
   *
   * @param line String in the file
   */
  public void parse(String line) {
    line = line.replaceAll("\\s+", "");
    time++;
    if (line.startsWith("begin")) {
      parseBegin(line);
    } else if (line.startsWith("end")) {
      parseEnd(line);
    } else if (line.startsWith("dump")) {
      parseDump(line);
    } else if (line.contains("R(")) {

    } else if (line.contains("W(")) {

    }
  }

  private void parseBegin(String line) {

  }

  private void parseEnd(String line) {

  }

  private void parseDump(String line) {

  }
}
