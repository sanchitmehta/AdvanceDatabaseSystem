package ADBFinalProject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Gives the structure of variable handled by the
 * distributed variables.
 * <p>
 * <p>
 * Must specify index to create the object
 * </p>
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 * @see Site
 * @see Transaction
 * @see TransactionManager
 */
class Variable {

  //Transactions holding the read lock
  private Set<Integer> readLocks;

  //ADBFinalProject.Transaction holding the write lock
  private int writeLock;
  private int idx;
  private int val;

  /*
   * Initialises a variable at particular index
   * and a given value
   */
  Variable(int val, int idx) {
    this.val = val;
  }

  /*
     * Initialises a variable at particular index,
     * taking the default value as 10*index.
     */
  Variable(int idx) {
    this.idx = idx;
    this.val = 10 * idx;
    readLocks = new HashSet<>();
    writeLock = -1;
  }

  @Override
  public String toString() {
    return "Variable x" + idx;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Variable variable = (Variable) o;

    return idx == variable.idx;
  }

  @Override
  public int hashCode() {
    return idx;
  }

  void removeLocksOnTrasanction(List<Integer> transactions) {
    for (Integer tId : transactions) {
      if (readLocks.contains(tId)) {
        readLocks.remove(tId);
      }
    }
    if (transactions.contains(writeLock)) {
      writeLock = -1;
    }
  }

  boolean addReadLock(int tID) {
    return writeLock != tID && readLocks.add(tID);
  }

  boolean addWriteLock(int tID) {
    if (writeLock > 0) {
      return false;
    }
    writeLock = tID;
    return true;
  }

  void clearWriteLock() {
    writeLock = -1;
  }

  void clearAllReadLocks() {
    readLocks.clear();
  }

  void clearReadLockByTransaction(int tId) {
    if (!readLocks.contains(tId)) {
      return;
    }
    readLocks.remove(tId);
  }

  int getId() {
    return idx;
  }

  int getVal() {
    return val;
  }

  List<Integer> getReadLocks() {
    return new ArrayList<>(readLocks);
  }

  int getWriteLockTID() {
    return writeLock;
  }

  boolean hasWriteLock() {
    return writeLock > 0;
  }

  boolean hasReadLockByTransction(int tId) {
    return readLocks.contains(tId);
  }

  void setVarValue(int val) {
    this.val = val;
  }

  boolean updateValue(int val,int tId) {
    if(writeLock!=tId){
      return false;
    }
    this.val = val;
    return true;
  }

}
