package ADBFinalProject;

/**
 * Reads the input and parses it
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
public class ParseInput {

  private static int time;

  TransactionManager transactionManager = new TransactionManager();

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
    String transactionName = line.substring(line.indexOf("T") + 1, line.indexOf(")"));
    int transactionId = Integer.parseInt(transactionName);
    transactionManager.addTransaction(
        transactionId,
        new Transaction(transactionId, time));
  }

  private void parseEnd(String line) {

  }

  private void parseDump(String line) {

  }
}
