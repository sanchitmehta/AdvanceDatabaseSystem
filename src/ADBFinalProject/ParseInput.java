package ADBFinalProject;

/**
 * Reads the input and parses it
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
public class ParseInput {

  private static int time;

  private TransactionManager transactionManager = new TransactionManager();

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
    int transactionId = getTransactionId(line);
    transactionManager.addTransaction(
        transactionId,
        new Transaction(transactionId, time));
  }

  private void parseEnd(String line) {
    int transactionId = getTransactionId(line);
    if (!transactionManager.deleteTransaction(transactionId)) {
      System.out.println("The transaction does not exists");
    } else {
      System.out.println("Transaction T" + transactionId + " completed");
    }
  }

  private void parseDump(String line) {
    for (Site site : transactionManager.getSites()) {
      site.printData();
    }
  }

  private int getTransactionId(String line) {
    return Integer.parseInt(line.substring(line.indexOf("T") + 1, line.indexOf(")")));
  }
}