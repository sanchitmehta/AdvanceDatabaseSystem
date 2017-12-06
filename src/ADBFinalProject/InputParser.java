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
      parseRead(line);
    } else if (line.contains("W(")) {
      parseWrite(line);
    } else if (line.contains("fail")) {
      parseFail(line);
    } else if (line.contains("recover")) {
      parseRecover(line);
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
        new ReadOnlyTransaction(transactionId, time, transactionManager.getSites()));
    } else {
      transactionManager.addTransaction(
        transactionId,
        new Transaction(transactionId, time));
    }
  }

  private void parseEnd(String line) {
    int transactionId = getTransactionId(line);
    if (transactionManager.endTransaction(transactionId)) {
      System.out.println("Transaction T" + transactionId + " completed");
    } else if (transactionManager.endTransaction(transactionId)) {
      System.out.println("Read Only Transaction T" + transactionId + " completed");
    } else {
      System.out.println("The transaction does not exists");
    }
  }

  private void parseRead(String line) {
    int transactionId = Integer.parseInt(
      line.substring(
        line.indexOf("T") + 1,
        line.indexOf(",")));

    int variableId = Integer.parseInt(
      line.substring(
        line.indexOf("x") + 1,
        line.indexOf(")")));
    transactionManager.executeReadOperation(transactionId, variableId);
  }

  private void parseWrite(String inp) {
    int transactionId = Integer.parseInt(
      inp.substring(
        inp.indexOf("T") + 1,
        inp.indexOf(",")));

    int variableId = Integer.parseInt(
      inp.substring(
        inp.indexOf("x") + 1,
        inp.lastIndexOf(",")));

    int value = Integer.parseInt(
      inp.substring(
        inp.lastIndexOf(",") + 1,
        inp.indexOf(")")));

    if (transactionManager.isAbortedTransaction(transactionId)) {
      System.out.println("Could not complete operation : " + inp
        + "\nThe Transaction T" +
        transactionId +
        "has " +
        "already been aborted");
    }
    transactionManager.executeWriteOperation(transactionId, variableId, value);
  }

  private void parseFail(String inp) {
    int siteIdx = getSiteIdx(inp);
    System.out.println("The site " + siteIdx + " has failed");
    transactionManager.failSite(siteIdx);
  }

  private void parseRecover(String inp) {
    int siteIdx = getSiteIdx(inp);
    System.out.println("Recovering site " + siteIdx);
    transactionManager.recoverSite(siteIdx);
  }

  private int getTransactionId(String line) {
    return Integer.parseInt(
      line.substring(
        line.indexOf("T") + 1,
        line.indexOf(")")));
  }

  private int getSiteIdx(String line) {
    return Integer.parseInt(
      line.substring(
        line.indexOf("(") + 1,
        line.indexOf(")")));
  }
}