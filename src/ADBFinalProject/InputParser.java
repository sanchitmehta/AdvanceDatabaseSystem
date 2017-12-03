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

    // if transaction has been already aborted then do not do anything
    if (transactionManager.isAbortedTransaction(transactionId)) {
      System.out.println("The Transaction T" + transactionId + "has already been aborted");
    }
    // if the transaction is a Read Only Transaction, then get the value and print it
    if (transactionManager.isReadOnlyTransRunning(transactionId)) {
      ReadOnlyTransaction readOnlyTransaction =
          (ReadOnlyTransaction) transactionManager
              .indexToTransactionMap
              .get(transactionId);
      Integer variableValue = readOnlyTransaction.getVariableValue(transactionId);
      if (variableValue != null) {
        System.out.println(readOnlyTransaction.toString() + " reads x" + variableId
            + "=" + variableValue);
      } else {
        System.out.println(readOnlyTransaction.toString() + " can't read x"
            + variableId + " because it is not available ");
      }
    }
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
      System.out.println("Could not complete opeation : " + inp
          + "\nThe Transaction T" +
          transactionId +
          "has " +
          "already been aborted");
    }
    transactionManager.executeWriteOperation(transactionId, variableId, value);
  }

  private int getTransactionId(String line) {
    return Integer.parseInt(
        line.substring(
            line.indexOf("T") + 1,
            line.indexOf(")")));
  }
}