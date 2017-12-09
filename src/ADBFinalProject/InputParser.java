package ADBFinalProject;

/**
 * Reads the input and parses it
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
class InputParser {

  private static int time;

  private TransactionManager transactionManager;
  private OutputHandler outputHandler;

  InputParser() {
    this.transactionManager = new TransactionManager();
    this.outputHandler = new OutputHandler(transactionManager);
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
      outputHandler.dump(line);
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
    if (!isValidTiD(transactionId)) {
      return;
    }
    if (!transactionManager.containsTransaction(transactionId)) {
      System.out.println("Error: Unrecognised Transaction. Ignoring");
      return;
    }
    transactionManager.endTransaction(transactionId);
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
    if (!isValidTiD(transactionId)) {
      return;
    }
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
          "has already been aborted");
    }
    transactionManager.executeWriteOperation(transactionId, variableId, value);
  }

  private void parseFail(String inp) {
    int siteIdx = getSiteIdx(inp);
    transactionManager.failSite(siteIdx);
  }

  private void parseRecover(String inp) {
    int siteIdx = getSiteIdx(inp);
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

  private boolean isValidTiD(int tId) {
    if (!transactionManager.containsTransaction(tId)) {
      System.out.println("Error: Unrecognised Transaction. Ignoring");
      return false;
    }
    return true;
  }
}