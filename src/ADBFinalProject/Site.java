package ADBFinalProject;

import java.util.*;

/**
 *
 * Describes each distributed site that handles
 * the read and write on variables
 *
 * @author    Sanchit Mehta, Pranav Chapekhar
 * @see       Site
 * @see       Transaction
 * @see       TransactionManager
 *
 */
public class Site {

  private int id;
  private Map<Integer, Variable> indexToVarMap;
  private Map<Integer, Transaction> runningTransactionsMap;
  private boolean isSiteRunning;

  public Site(int id) {
    this.indexToVarMap = new HashMap<>();
    this.isSiteRunning = true;
    this.runningTransactionsMap = new HashMap<>();
    this.id = id;
  }

  public void failCurrentSite(){
    for(Variable v:indexToVarMap.values()){
      v.removeLocksOnTrasanction(new ArrayList<>(runningTransactionsMap.values()));
    }
    this.isSiteRunning=false;
  }

  public boolean isSiteRunning(){
    return this.isSiteRunning;
  }

  /**
   * Adds read lock on a particular transaction and variable held
   * by this site
   *
   * @param tID transaction id on which read lock is to be added
   * @param vID variable id on on which read lock is to be added
   * @return true if was successfully able to add read
   *         lock on transaction tID
   *         false if there is already a read lock on
   *         this transaction/site does have this transaciton/var
   *
   */
  public boolean createReadLockOnTransaction(int tID,int vID){
    if(!runningTransactionsMap.containsKey(tID)
      ||!indexToVarMap.containsKey(vID)){
      return false;
    }
    Transaction t = runningTransactionsMap.get(tID);
    Variable v = indexToVarMap.get(vID);
    return v.addReadLock(t);
  }


  /**
   * Adds read lock on a particular transaction and variable held
   * by this site
   *
   * @param tID transaction id on which read lock is to be added
   * @param vID variable id on on which read lock is to be added
   * @return true if was successfully able to add read
   *         lock on transaction tID
   *         false if there is already a read lock on
   *         this transaction/site does have this transaciton/var
   *
   */
  public boolean createWriteLockOnTransaction(int tID,int vID){
    if(!runningTransactionsMap.containsKey(tID)
      ||!indexToVarMap.containsKey(vID)){
      return false;
    }
    Transaction t = runningTransactionsMap.get(tID);
    Variable v = indexToVarMap.get(vID);
    return v.addWriteLock(t);
  }

  public int getId() {
    return id;
  }

}
