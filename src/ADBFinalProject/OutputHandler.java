package ADBFinalProject;

import java.util.Map;

/**
 * Parses the state of the Database and
 * displays the output
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 *
 */
class OutputHandler {

  private TransactionManager transactionManager;

  OutputHandler(TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  /**
   * @param line the parsed input line
   */
  void dump(String line) {
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

  /**********************************
   Private Helper Methods
   **********************************/
  private void dump() {
    System.out.println("\n\nThe output of dump is as follows:  \n \n ");
    System.out.print(String.format("%12s", "|"));
    System.out.print("");
    Site[] sites = transactionManager.getSites();
    for (int i = 1; i <= TransactionManager.NUMBER_OF_VARIABLES; i++) {
      String print = String.format("%4s", "x" + i);
      System.out.print(print + "|");
    }
    System.out.println();
    for (int i = 1; i < sites.length; i++) {
      System.out.println(
          "---------------------------------------------------------------------------------------------------------------");
      dumpAtSite(sites[i]);
    }
  }

  private void dumpAtSite(Site site) {
    StringBuilder sb = new StringBuilder(String.format("%8s", site.toString()));
    sb.append(String.format("%4s", "|"));
    Map<Integer, Variable> indexToVarMap = site.getIndexToVarMap();
    for (int variableIdx = 1; variableIdx <= TransactionManager.NUMBER_OF_VARIABLES;
        variableIdx++) {
      if (!indexToVarMap.containsKey(variableIdx)) {
        sb.append(String.format("%4s", "  "));
        sb.append("|");
      } else {
        sb.append(String.format("%4s", indexToVarMap.get(variableIdx).getVal()));
        sb.append("|");
      }
    }
    System.out.println(String.format("%6s", sb.toString()));
  }

  private void dumpxAtAllSites(int variableIdx) {
    Site[] sites = transactionManager.getSites();
    for (int i = 1; i < sites.length; i++) {
      Variable variable = sites[i].getVariableByIndex(variableIdx);
      if (variable != null) {
        System.out.println(variable.toString() + " " + variable.getVal());
      } else {
        System.out.println(
            "Variable x" + variableIdx + " is not present at site " + sites[i].toString());
      }
    }
  }
}
