package ADBFinalProject;

import java.util.Map;

class DumpOutput {

  private TransactionManager transactionManager;

  DumpOutput(TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  void parseDump(String line) {
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
    for (Site site : transactionManager.getSites()) {
      dumpAtSite(site);
    }
  }

  private void dumpAtSite(Site site) {
    System.out.println(site.toString());
    Map<Integer, Variable> indexToVarMap = site.getIndexToVarMap();
    for (Integer index : indexToVarMap.keySet()) {
      System.out.println("x" + index + " : " + indexToVarMap.get(index).getVal());
    }
  }

  private void dumpxAtAllSites(int variableIdx) {
    for (Site site : transactionManager.getSites()) {
      Variable variable = site.getVariableByIndex(variableIdx);
      if (variable != null) {
        System.out.println(variable.toString() + " " + variable.getVal());
      } else {
        System.out.println(
            "Variable x" + variableIdx + " is not present at site " + site.toString());
      }
    }
  }
}
