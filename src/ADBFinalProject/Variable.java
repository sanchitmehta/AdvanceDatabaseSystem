package ADBFinalProject;

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
public class Variable {

  //Transactions holding the read lock
  Set<Transaction> readLocks;

  //ADBFinalProject.Transaction holding the write lock
  Transaction writeLock;

  int idx;

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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Variable variable = (Variable) o;

    return idx == variable.idx;

  }

  @Override
  public int hashCode() {
    return idx;
  }

  public void removeLocksOnTrasanction(List<Transaction> transactions) {
    for (Transaction t : transactions) {
      if (readLocks.contains(t)) {
        readLocks.remove(t);
      }
    }
    if (transactions.contains(writeLock)) {
      writeLock = null;
    }
  }

  public boolean addReadLock(Transaction t){
    if(writeLock==t){
      return false;
    }
    return readLocks.add(t);
  }

  public boolean addWriteLock(Transaction t){
    if(writeLock!=null){
      return false;
    }
    writeLock=t;
    return true;
  }

  void clearWriteLock(){
    writeLock=null;
  }

  void clearReadLockWith(){
    readLocks.clear();
  }


}
