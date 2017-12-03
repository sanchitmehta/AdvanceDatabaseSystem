package ADBFinalProject;

import java.util.*;

class TransactionManager {

  private static final int NUMBER_OF_SITES = 10;
  private Map<Integer, Transaction> indexToTransactionMap;
  private Set<Integer> runningTransactions;
  private Set<Integer> readOnlyRunningTransactions;
  private Set<Integer> abortedTransactions;
  private Site[] sites;
  private DeadlockHandler deadlockHandler;
  private List<Integer> waitList;
  private List<Operation> pendingOperations;

  TransactionManager() {
    this.runningTransactions = new HashSet<>();
    this.readOnlyRunningTransactions = new HashSet<>();
    this.abortedTransactions = new HashSet<>();
    deadlockHandler = new DeadlockHandler();
    pendingOperations = new LinkedList<>();
    waitList = new ArrayList<>();
    createSites();
  }

  private void createSites() {
    this.sites = new Site[NUMBER_OF_SITES + 1];
    for (int i = 1; i < NUMBER_OF_SITES + 1; i++) {
      sites[i] = new Site(i);
    }
  }

  /**
   * Adds the transaction to the running Transactions map to keep a track of currently running
   * transactions
   *
   * @param tId transaction Id
   * @param transaction {@code Transaction}
   */
  void addTransaction(Integer tId, Transaction transaction) {
    System.out.println(transaction.toString() + " has begun!");
    runningTransactions.add(tId);
    indexToTransactionMap.put(tId, transaction);
  }

  /**
   * Adds the transaction to the Read Only Transactions map to keep a track of currently running
   * transactions
   *
   * @param tId transaction Id
   * @param readOnlyTransaction {@code Transaction}
   */
  void addReadOnlyTransaction(Integer tId, ReadOnlyTransaction readOnlyTransaction) {
    indexToTransactionMap.put(tId, readOnlyTransaction);
    readOnlyRunningTransactions.add(tId);
  }

  /**
   * Ends a transaction by removing it from the running Transaction map
   *
   * @param tId transaction Id
   * @return true if deletion is successful, false otherwise
   */
  boolean endTransaction(int tId) {
    if (runningTransactions.contains(tId)) {
      Transaction transaction = indexToTransactionMap.get(tId);
      indexToTransactionMap.remove(tId);
      runningTransactions.remove(tId);
      return transaction.endTransaction();
    }
    return false;
  }

  /**
   * Ends a read only transaction and removes it from the  Read Only Transaction
   *
   * @param tId transaction Id
   * @return true if deletion is successful, false otherwise
   */
  boolean endReadOnlyTransaction(int tId) {
    if (readOnlyRunningTransactions.contains(tId)) {
      readOnlyRunningTransactions.remove(tId);
      ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction) indexToTransactionMap.get(tId);
      return readOnlyTransaction.endTransaction();
    } else {
      return false;
    }
  }

  /*
   * Detects a deadlock and removes the deadlock by
   * aborting a transaction
   *
   */
  public void detectAndRemoveDeadlock() {

  }

  /**
   * Checks if the transaction is aborted or not
   *
   * @param tId transaction Id
   * @return true if transaction is aborted, false otherwise
   */
  boolean isAbortedTransaction(int tId) {
    return abortedTransactions.contains(tId);
  }

  /**
   * @return Array of sites
   */
  Site[] getSites() {
    return sites;
  }

  private void addPendingOperation(Operation op) {
    pendingOperations.add(op);
  }


  boolean executeWriteOperation(int tID, int vID, int varVal) {
    Operation op = new Operation(tID, vID, varVal);
    if (waitList.contains(tID)) {
      addPendingOperation(op);
      return false;
    }
    LockRequestStatus status = requestWriteLock(vID, tID);
    switch (status) {
      case TRANSACTION_CAN_GET_LOCK:
        //updateWriteLocks(vID, tID);
        indexToTransactionMap.get(tID).addOperation(op);
        break;
      case TRANSACTION_WAIT_LISTED:
        addPendingOperation(op);
        break;
      case ALL_SITES_DOWN:
        addPendingOperation(op);
        break;
    }
    return true;
  }

  private LockRequestStatus requestWriteLock(int vID, int tID) {
    //Assuming by default the site is down
    LockRequestStatus status = LockRequestStatus.ALL_SITES_DOWN;
    Transaction requestT = indexToTransactionMap.get(tID);
    int lockingTID;
    boolean atleastOneSiteIsRunning = false;
    for (int i = 1; i < sites.length; i++) {
      if (sites[i].hasVariable(vID)) {
        if (sites[i].isRunning() || sites[i].isReadyForRecovery()) {
          atleastOneSiteIsRunning = true;
          break;
        }
      }
    }

    if (atleastOneSiteIsRunning) {
      for (int i = 1; i < sites.length; i++) {
        status = LockRequestStatus.TRANSACTION_CAN_GET_LOCK;
        if (!sites[i].hasVariable(vID)) {
          continue;
        }
        List<Integer> readlockTransations = sites[i].getReadLockSet(vID);
        if (!readlockTransations.isEmpty()) {
          if (readlockTransations.size() == 1) {
            int currTID = readlockTransations.get(0);
            if (currTID == tID) {
              return status;
            } else {
              return waitDieProtocol(currTID, tID);
            }
          } else {
            int oldestTID = getOldestTID(readlockTransations);
            LockRequestStatus waitDieResult = waitDieProtocol(oldestTID, tID);
            if (waitDieResult == LockRequestStatus.TRANSACTION_WAIT_LISTED
              || waitDieResult == LockRequestStatus.TRANSACTION_ABORTED) {
              return waitDieResult;
            }
          }
        }

        lockingTID = sites[i].getWriteLockTIDForVariable(vID);
        if (lockingTID > 0 && lockingTID != tID) {
          LockRequestStatus waitDieResult = waitDieProtocol(lockingTID, tID);
          if (waitDieResult == LockRequestStatus.TRANSACTION_WAIT_LISTED
            || waitDieResult == LockRequestStatus.TRANSACTION_ABORTED) {
            return waitDieResult;
          }
        }
      }
    }
    if (status == LockRequestStatus.ALL_SITES_DOWN) {
      waitList.add(tID);
      System.out.println(tID + " moved to waitList.");
      runningTransactions.remove(tID);
    }
    return status;
  }

  private LockRequestStatus waitDieProtocol(int lockingTID, int requestTID) {

    Transaction requestT, lockingT;
    requestT = indexToTransactionMap.get(requestTID);
    lockingT = indexToTransactionMap.get(lockingTID);

    if (lockingT != null
      && lockingT.getStartTime() < requestT.getStartTime()) {
      String message = "Abort " + requestTID + " for Wait-die by "
        + lockingTID;
      //abort(requestTID, message);
      return LockRequestStatus.TRANSACTION_ABORTED;
    } else if (lockingT != null) {
      //waitgraph.addEdge(requestTID, lockingTID);
      waitList.add(requestTID);
      System.out.println(requestTID + " moved to waitList.");
      runningTransactions.remove(requestTID);
      return LockRequestStatus.TRANSACTION_WAIT_LISTED;
    }
    return LockRequestStatus.TRANSACTION_CAN_GET_LOCK;
  }

  private int getOldestTID(List<Integer> readlockTs) {
    int oldTID = -1;
    Iterator<Integer> readit = readlockTs.iterator();
    if (readit.hasNext()) {
      oldTID = readit.next();
      Transaction oldT = indexToTransactionMap.get(oldTID);
      int currTID;
      while (readit.hasNext()) {
        currTID = readit.next();
        Transaction currT;
        currT = indexToTransactionMap.get(currTID);

        if (currT != null && oldT.getStartTime() > currT.getStartTime()) {
          oldTID = currTID;
          oldT = currT;
        }
      }
    }
    return oldTID;
  }

  private boolean abort(Set<Integer> abortTIDset, String message) {
    Transaction abortT;
    //Set<String> resumeTIDs = new HashSet<String>();
    /*
    if (this.VERBOSE) {
      System.out.println(message);
    }
    */
    for (int abortTID : abortTIDset) {
      if (abortTID > 0) {
        if (runningTransactions.contains(abortTID)) {
          abortT = indexToTransactionMap.get(abortTID);
          runningTransactions.remove(abortTID);
          abortedTransactions.add(abortTID);
        }
        if (waitList.contains(abortTID)) {
          abortT = indexToTransactionMap.get(abortTID);
          waitList.remove(abortTID);
          abortedTransactions.add(abortTID);
        }
        //resumeTIDs.addAll(deadlockHandler.getWaitedBySet(abortTID));
        //deadlockHandler.clearEdge(abortTID);
        //clearLocksByTID(abortTID);
      }
    }

    //resumeWaitingTIDs(resumeTIDs);
    //runPendingOperations();
    return true;
  }

  private boolean abort(Integer abortTID, Integer message) {
    Set<Integer> abortTIDset = new HashSet<>();
    abortTIDset.add(abortTID);
    //abort(abortTIDset, message);
    return true;
  }

}