package ADBFinalProject;

/**
 * Reads the input and parses it
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
class InputParser {

  private static int time;

  private TransactionManager transactionManager;
  private DumpOutput dumpOutput;

  InputParser() {
    this.transactionManager = new TransactionManager();
    this.dumpOutput = new DumpOutput(transactionManager);
  }

  /**
   * Parses a single line from the text file
   *
   * @param line String in the file
   */
  void parse(String line) {
    line = line.replaceAll("\\s+", "");
    time++;
    if (line.startsWith("begin")) {
      parseBegin(line);
    } else if (line.startsWith("end")) {
      parseEnd(line);
    } else if (line.startsWith("dump")) {
      dumpOutput.parseDump(line);
    } else if (line.contains("R(")) {

    } else if (line.contains("W(")) {

    }
  }

  /**********************************
   Private Helper Methods
   **********************************/

  private void parseBegin(String line) {
    int transactionId = getTransactionId(line);
    if (line.contains("RO")) {
      transactionManager.addReadOnlyTransaction(
          transactionId,
          new ReadOnlyTransaction(transactionId, time));
    } else {
      transactionManager.addTransaction(
          transactionId,
          new Transaction(transactionId, time));
    }
  }

  private void parseEnd(String line) {
    int transactionId = getTransactionId(line);
    if (transactionManager.deleteTransaction(transactionId)) {
      System.out.println("Transaction T" + transactionId + " completed");
    } else {
      System.out.println("The transaction does not exists");
    }
  }

  private int getTransactionId(String line) {
    return Integer.parseInt(line.substring(line.indexOf("T") + 1, line.indexOf(")")));
  }
}