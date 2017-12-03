package ADBFinalProject;

import dnl.utils.text.table.TextTable;
import java.util.Map;

/**
 * Displays the output
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 */
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

//  private void dump() {
//    Site[] sites = transactionManager.getSites();
//    for (int i = 1; i < sites.length; i++) {
//      dumpAtSite(sites[i]);
//    }
//  }

//  private void dump() {
//    Site[] sites = transactionManager.getSites();
//    Object[][] data =
//        new Object[TransactionManager.NUMBER_OF_SITES][TransactionManager.NUMBER_OF_VARIABLES]; // sites will be the columns and variables will be the rows
//    for (int siteIdx = 1; siteIdx <= data.length; siteIdx++) {
//      Site current = sites[siteIdx];
//      Map<Integer, Variable> indexToVarMap = current.getIndexToVarMap();
//      for (int variableIdx = 1; variableIdx <= data[0].length; variableIdx++) {
//        Variable variable = indexToVarMap.get(variableIdx);
//        if (variable == null) {
//          data[siteIdx - 1][variableIdx - 1] = " ";
//        } else {
//          data[siteIdx - 1][variableIdx - 1] = variable.getVal();
//        }
//      }
//    }
//    String[] columnNames = {"Site 1", "Site 2", "Site 3", "Site 4", "Site 5", "Site 6", "Site 7",
//        "Site 8", "Site 9", "Site 10", "Site 11", "Site 12", "Site 13", "Site 14", "Site 15", "Site 16",
//        "Site 17", "Site 18", "Site 19", "Site 20"};
//    TextTable textTable = new TextTable(columnNames, data);
//    textTable.setAddRowNumbering(true);
//    textTable.setSort(0);
//    textTable.printTable();
//  }
  public void dump() {
//    System.out.println("Variables -> ");
//    System.out.println("Sites ");
//    System.out.println("|");
//    System.out.println("\\/");
    System.out.println(" The output of dump is as follows:  \n \n ");
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
    StringBuffer sb = new StringBuffer(String.format("%8s", site.toString()));
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
