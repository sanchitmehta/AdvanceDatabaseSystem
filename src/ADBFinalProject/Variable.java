package ADBFinalProject;

import java.util.ArrayList;
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
  private Integer writeLock;
  private int idx;
  private int val;

  /*
   * Initialises a variable at particular index
   * and a given value
   */
  public Variable(int val, int idx) {
    this.val = val;
  }

  /*
     * Initialises a variable at particular index,
     * taking the default value as 10*index.
     */
  public Variable(int idx) {
    this.idx = idx;
    this.val = 10 * idx;
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
    writeLock = null;
  }

  void clearReadLockWith() {
    readLocks.clear();
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

}
