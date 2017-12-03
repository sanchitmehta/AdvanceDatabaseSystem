package ADBFinalProject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes each distributed site that handles
 * the read and write on variables
 *
 * @author Sanchit Mehta, Pranav Chapekhar
 * @see Site
 * @see Transaction
 * @see TransactionManager
 */
class Site {

  private int id;

  private Map<Integer, Variable> indexToVarMap;
  private boolean isSiteRunning;

  Site(int id) {
    this.indexToVarMap = new HashMap<>();
    this.isSiteRunning = true;
    this.id = id;
    setDefaultVariable(20);
  }

  boolean hasVariable(int varIdx) {
    return indexToVarMap.containsKey(varIdx);
  }

  boolean setVariableValue(int varIdx, int varVal) {
    if (!hasVariable(varIdx)) {
      return false;
    }
    indexToVarMap.get(varIdx).updateValue(varVal);
    return true;
  }

  void failCurrentSite() {
    for (Variable v : indexToVarMap.values()) {
      //v.removeLocksOnTrasanction(new ArrayList<>(runningTransactionsMap.keySet()));
    }
    this.isSiteRunning = false;
  }

  boolean isRunning() {
    return this.isSiteRunning;
  }

  /**
   * Adds read lock on a particular transaction and variable held
   * by this site
   *
   * @param tID transaction id on which read lock is to be added
   * @param vID variable id on on which read lock is to be added
   * @return true if was successfully able to add read lock on transaction tID false if there is
   * already a read lock on this transaction/site does have this transaciton/var
   */
  boolean createReadLockOnTransaction(int tID, int vID) {
    if (!indexToVarMap.containsKey(vID)) {
      return false;
    }
    Variable v = indexToVarMap.get(vID);
    return v.addReadLock(tID);
  }


  /**
   * Adds read lock on a particular transaction and variable held
   * by this site
   *
   * @param tID transaction id on which read lock is to be added
   * @param vID variable id on on which read lock is to be added
   * @return true if was successfully able to add read lock on transaction tID false if there is
   * already a read lock on this transaction/site does have this transaciton/var
   */
  boolean createWriteLockOnTransaction(int tID, int vID) {
    if (!indexToVarMap.containsKey(vID)) {
      return false;
    }
    Variable v = indexToVarMap.get(vID);
    return v.addWriteLock(tID);
  }

  /**
   * Checks if the variable with given index is present at this site
   *
   * @param variableIdx Index of the variable
   * @return Variable if present, null otherwise
   */
  Variable getVariableByIndex(int variableIdx) {
    return indexToVarMap.getOrDefault(variableIdx, null);
  }

  /**
   * Getter for IndexToVariable Map
   *
   * @return Map
   */
  Map<Integer, Variable> getIndexToVarMap() {
    return indexToVarMap;
  }

  @Override
  public String toString() {
    return "Site " + id;
  }

  public int getSiteIndex() {
    return this.id;
  }

  public Variable getVariableValue(int vIdx) {
    return indexToVarMap.get(vIdx);
  }

  int getWriteLockTIDForVariable(int vIdx) {
    Variable v = indexToVarMap.get(vIdx);
    if (v == null || !v.hasWriteLock()) {
      return -1;
    }
    return indexToVarMap.get(vIdx).getWriteLockTID();
  }

  public boolean isReadLockedByVariable(int vIdx) {
    return indexToVarMap.get(vIdx).getReadLocks().isEmpty();
  }

  public boolean isReadLockedByTransaction(int vIdx, int tId) {
    return !indexToVarMap.get(vIdx).getReadLocks().isEmpty()
      && indexToVarMap.get(vIdx).getReadLocks().contains(tId);
  }

  private void setDefaultVariable(int numVar) {
    for (int varIndex = 1; varIndex <= numVar; varIndex++) {
      if (varIndex % 2 == 0) {
        Variable var = new Variable(varIndex);
        this.indexToVarMap.put(varIndex, var);
      } else if ((varIndex + 1) % 10 == id) {
        Variable var = new Variable(varIndex);
        this.indexToVarMap.put(varIndex, var);
      }
    }
//    Set<Integer> replicatedSite = new HashSet<Integer>();
//    replicatedSite.add(this.getId());
//    for (Integer varIndex : indexToVarMap.keySet()) {
//      if (indexToVarMap.containsKey(varIndex)) {
//        varReplicationLookup.get(varIndex).addAll(replicatedSite);
//      } else {
//        varReplicationLookup.put(varIndex, replicatedSite);
//      }
//    }
  }

  boolean isReadyForRecovery() {
//    if (!this.isSiteRunning && this.isRecovered != null && this.isRecovered) {
//      return true;
//    }
//    return false;
    return true;
  }


  /*
   *
   */
  List<Integer> getReadLockSet(int vIdx) {
    return indexToVarMap.get(vIdx).getReadLocks();
  }

  boolean updateWriteLock(int varIndex, int tranID) {
    if (!this.isSiteRunning) {
      return false;
    }
    if (!indexToVarMap.containsKey(varIndex)) {
      return false;
    }
    if (this.isWriteLocked(varIndex)) {
      return this.isWriteLockedBy(varIndex, tranID);
    } else {
      this.indexToVarMap.get(varIndex).addWriteLock(tranID);
      //this.createReadLockOnTransaction(tranID,varIndex);
      return true;
    }
  }

  private boolean isWriteLocked(int varIndex) {
    return this.indexToVarMap.get(varIndex).hasWriteLock();
  }

  private boolean isWriteLockedBy(int varIndex, int tranID) {
    return !isWriteLocked(varIndex)
      || this.indexToVarMap.get(varIndex).getWriteLockTID() == tranID;
  }

  boolean commitVariableValue(int vIdx, int newVarVal) {
    if (!isSiteRunning || !indexToVarMap.containsKey(vIdx)) {
      return false;
    }
    Variable v = indexToVarMap.get(vIdx);
    v.updateValue(newVarVal);
    return true;
  }

  boolean executeTransaction(int tId) {
    if (!this.isSiteRunning) {
      return false;
    }
    /*
    for (Operation opTemp : runningTransactionsMap.get(tId).getPendingOperations()) {
      if (opTemp.isWriteOperation()) {
        int newValue = opTemp.getVariableVal();
        int varIndex = opTemp.getVariableId();
        Variable v = indexToVarMap.get(varIndex);
        v.updateValue(newValue);
        //(newValue, varIndex, T.getTID());
      }
    }
    this.clearLocksOf(tId);

    return true;
    */
    return true;
  }

  void clearLocksOf(int tranID) {

    for (Integer varIndex : getAllAvailableVarIndex()) {
      if (this.isWriteLockedBy(varIndex, tranID)) {
        this.emptyLocks(varIndex);
      }
    }
  }

  private void emptyLocks(int varIndex) {
    this.indexToVarMap.get(varIndex).clearAllLocks();
  }

  private Set<Integer> getAllAvailableVarIndex() {
    return this.indexToVarMap.keySet();
  }

//  public String printData(int numVar) {
//    String siteTitle = String
//        .format("%4s", "S" + this.getSiteIndex() + ":");
//    StringBuffer datastring = new StringBuffer(siteTitle);
//    for (int i = 1; i <= numVar; i++) {
//      if (indexToVarMap.containsKey(i)) {
//        String value = String.format("%5s", indexToVarMap.get(i).getVal());
//        datastring.append(value + "|");
//      } else {
//        String value = String.format("%6s", "|");
//        datastring.append(value);
//      }
//    }
//    //datastring.append("\n Status: " + this.status);
//    return datastring.toString();
//  }

  /**********************************
   Private Helper Methods
   **********************************/

  private int getId() {
    return id;
  }

}
