package ADBFinalProject;

/**
 * Reads the input and parses it
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
class ParseInput {

  private static int time;

  private TransactionManager transactionManager = new TransactionManager();

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
      parseDump(line);
    } else if (line.contains("R(")) {

    } else if (line.contains("W(")) {

    }
  }

  /**********************************
   Private Helper Methods
   **********************************/

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
    if (line.equalsIgnoreCase("dump()")) {
      dump();
    } else if (line.startsWith("dump(x")) {
      int variableIdx = Integer.parseInt(
          line.substring(
              line.indexOf("x") + 1,
              line.indexOf(")")));
      dumpxAtAllSites(variableIdx);
    } else {
      int siteNumber = Integer.parseInt(
          line.substring(
              line.indexOf("(") + 1,
              line.indexOf(")")));
      dumpAtSite(transactionManager.getSites()[siteNumber]);
    }
  }

  private void dump() {
    for (Site site : transactionManager.getSites()) {
      site.printData();
    }
  }

  private void dumpAtSite(Site site) {
    site.printData();
  }

  private void dumpxAtAllSites(int variableIdx) {
    for (Site site : transactionManager.getSites()) {
      Variable variable = site.getVariableByIndex(variableIdx);
      if (variable == null) {
        System.out.println(
            "Variable x" + variableIdx + " is not present at site " + site.toString());
      } else {
        System.out.println(variable.toString() + " " + variable.getVal());
      }
    }
  }

  private int getTransactionId(String line) {
    return Integer.parseInt(line.substring(line.indexOf("T") + 1, line.indexOf(")")));
  }
}