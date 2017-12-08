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

  //Transactions holding the read lock on this variable
  private Set<Integer> readLocks;

  //Transaction holding the write lock
  //Only one transaction can have the write lock on this variable
  private int writeLock;
  private int idx;
  private int val;

  /**
   * Initializes the variable at particular index by taking the default value as 10 X index
   *
   * @param idx index of this variable
   */
  Variable(int idx) {
    this.idx = idx;
    this.val = 10 * idx;
    readLocks = new HashSet<>();
    writeLock = -1;
  }

  /**
   * Adds the read Lock on the transaction
   *
   * @param tID The transaction Id to add the read lock on
   * @return true if successful; false otherwise
   */
  boolean addReadLock(int tID) {
    return writeLock != tID && readLocks.add(tID);
  }

  /**
   * Adds the write lock on the transaction
   *
   * @param tID The transaction Id to add the write lock on
   * @return true if successful; false otherwise
   */
  boolean addWriteLock(int tID) {
    if (writeLock > 0) {
      return false;
    }
    writeLock = tID;
    return true;
  }

  /**
   * Resets to the default value of the write lock
   */
  void clearWriteLock() {
    writeLock = -1;
  }

  /**
   * Clears all the read locks
   */
  void clearAllReadLocks() {
    readLocks.clear();
  }

  /**
   * Remove the read lock of a particular transaction Id
   *
   * @param tId The transaction Id whose lock needs to be removed
   */
  void clearReadLockByTransaction(int tId) {
    if (!readLocks.contains(tId)) {
      return;
    }
    readLocks.remove(tId);
  }

  /**
   * Getter method for the value of this variable
   *
   * @return value of this variable
   */
  int getVal() {
    return val;
  }

  /**
   * Getter method for returning the list of read locks
   *
   * @return List of read locks
   */
  List<Integer> getReadLocks() {
    return new ArrayList<>(readLocks);
  }

  /**
   * Getter method for returning the transaction Id that has the write lock on this variable
   *
   * @return The transaction Id or -1 if no transaction has the read lock
   */
  int getWriteLockTID() {
    return writeLock;
  }

  /**
   * Returns true if this variable has the write lock
   *
   * @return true if this has the lock, else return false
   */
  boolean hasWriteLock() {
    return writeLock > 0;
  }

  /**
   * Returns true if variable has the read lock by tID
   *
   * @param tId transaction Id
   * @return true if read lock has been acquired by the tID; false otherwise
   */
  boolean hasReadLockByTransction(int tId) {
    return readLocks.contains(tId);
  }

  /**
   * Setter method for setting the value of the variable
   *
   * @param val sets the variable value
   */
  void setVarValue(int val) {
    this.val = val;
  }

  /**
   * Updates the value
   *
   * @param val new value
   * @param tId transaction Id
   * @return true if successful; false otherwise
   */
  boolean updateValue(int val, int tId) {
    if (writeLock != tId) {
      return false;
    }
    this.val = val;
    return true;
  }

  /**
   * Checks if it has he readlock
   *
   * @return true if read lock exists, false otherwise
   */
  boolean hasReadLocks() {
    return !readLocks.isEmpty();
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
    int result = readLocks != null ? readLocks.hashCode() : 0;
    result = 31 * result + writeLock;
    result = 31 * result + idx;
    result = 31 * result + val;
    return result;
  }
}