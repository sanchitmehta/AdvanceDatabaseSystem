package ADBFinalProject;

/**
 * Read Only Transaction takes the snapshot of the system that has been last committed before this
 * transaction begins
 */
class ReadOnlyTransaction extends Transaction {

  private Integer[] variableValuesAtStart;
  private static final int NUMBER_OF_VARIABLES = 20;
  private Site[] sites;

  ReadOnlyTransaction(int transactionId, int startTime, Site[] sites) {
    super(transactionId, startTime);
    this.sites = sites;
    variableValuesAtStart = new Integer[NUMBER_OF_VARIABLES + 1];
    populateMap();
  }

  @Override
  public String toString() {
    return "ReadOnlyTransaction{" + super.getId() + "}";
  }

  /**
   * Gets the value of the variable
   *
   * @param variableIndex the index of the variable
   * @return the value of the variable at that index; null otherwise
   */
  Integer getVariableValue(int variableIndex) {
    if (variableIndex < variableValuesAtStart.length) {
      return variableValuesAtStart[variableIndex];
    } else {
      return null;
    }
  }

  /**********************************
   Private Helper Methods
   **********************************/

  private void populateMap() {
    for (int variableIdx = 1; variableIdx < variableValuesAtStart.length; variableIdx++) {
      for (int siteIdx = 1; siteIdx < sites.length; siteIdx++) {
        if (sites[siteIdx].isRunning()
            && sites[siteIdx].getIndexToVarMap().containsKey(variableIdx)) {
          variableValuesAtStart[variableIdx] =
              sites[siteIdx]
                  .getIndexToVarMap()
                  .get(variableIdx)
                  .getVal();
          break;
        }
      }
    }
  }

}