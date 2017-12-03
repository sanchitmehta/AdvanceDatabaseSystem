package ADBFinalProject;

import java.util.*;

class TransactionManager {

  static final int NUMBER_OF_SITES = 10;
  static final int NUMBER_OF_VARIABLES = 20;
  Map<Integer, Transaction> indexToTransactionMap;
  private Set<Integer> runningTransactions;
  private Set<Integer> readOnlyRunningTransactions;
  private Set<Integer> abortedTransactions;
  private Set<Integer> endedTransaction;
  private Site[] sites;
  private DeadlockHandler deadlockHandler;
  private List<Integer> waitList;
  private List<Operation> pendingOperations;

  TransactionManager() {
    this.indexToTransactionMap = new HashMap<>();
    this.runningTransactions = new HashSet<>();
    this.readOnlyRunningTransactions = new HashSet<>();
    this.abortedTransactions = new HashSet<>();
    deadlockHandler = new DeadlockHandler();
    pendingOperations = new LinkedList<>();
    waitList = new ArrayList<>();
    createSites();
    endedTransaction = new HashSet<>();
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
    Transaction t;
    if (runningTransactions.contains(tId)) {
      t = indexToTransactionMap.get(tId);
      runningTransactions.remove(tId);
      //t.setEndTime(TIMER);
      endedTransaction.add(tId);
      commitTransaction(tId);
      System.out.println("Ending Transaction " + tId);
      Set<Integer> resumeTIDs = deadlockHandler.getTransactionsThatWaitFor(tId);
      deadlockHandler.clearEdge(tId);
      clearLocksByTID(tId);
      runPendingOperations();
      resumeWaitingTIDs(resumeTIDs);
      runPendingOperations();
    } else if (readOnlyRunningTransactions.contains(tId)) {
      readOnlyRunningTransactions.remove(tId);
      t = indexToTransactionMap.remove(tId);
      //t.setEndTime(TIMER);
      endedTransaction.add(tId);
    } else if (abortedTransactions.contains(tId)) {
      System.out.println(tId + " was aborted and thus cannot commit.");
    } else {
      addPendingOperation(new Operation());
    }
    return true;
  }

  private boolean commitTransaction(int tId) {
    for (int i = 1; i < sites.length; i++) {
      //sites[i].executeTransaction(tId);
      for (Operation opTemp : indexToTransactionMap.get(tId).getPendingOperations()) {
        if (opTemp.isWriteOperation()) {
          int newValue = opTemp.getVariableVal();
          int varIndex = opTemp.getVariableId();
          sites[i].commitVariableValue(varIndex, newValue);
          //v.updateValue(newValue);
          //(newValue, varIndex, T.getTID());
          sites[i].clearLocksOf(tId);
        }
      }
      //this.clearLocksOf(tId);
    }
    System.out.println("Transaction " + tId + " committed.");
    return true;
  }

  private boolean clearLocksByTID(int tId) {
    for (int i = 1; i < sites.length; i++) {
      sites[i].clearLocksOf(tId);
    }
    return true;
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
      ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction) indexToTransactionMap
          .get(tId);
      return readOnlyTransaction.endTransaction();
    } else {
      return false;
    }
  }

  /**
   * Checks if the read only transaction is running or not
   *
   * @return true if the read only transaction is running false otherwise
   */
  boolean isReadOnlyTransRunning(int tId) {
    return readOnlyRunningTransactions.contains(tId);
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
        updateWriteLocks(vID, tID);
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

  private boolean updateWriteLocks(int vId, int tID) {
    for (int i = 1; i < sites.length; i++) {
      sites[i].updateWriteLock(vId, tID);
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
      String message = "Abort " + requestTID + " as it's the youngest transaction by "
          + lockingTID;
      abort(requestTID, message);
      return LockRequestStatus.TRANSACTION_ABORTED;
    } else if (lockingT != null) {
      deadlockHandler.addTransactionEdge(requestTID, lockingTID);
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
    Set<Integer> resumeTransactions = new HashSet<>();
    System.out.println(message);
    /*
    if (this.VERBOSE) {
      System.out.println(message);
    }
    */
    for (int abortTID : abortTIDset) {
      if (abortTID > 0) {
        if (runningTransactions.contains(abortTID)) {
          runningTransactions.remove(abortTID);
          abortedTransactions.add(abortTID);
        }
        if (waitList.contains(abortTID)) {
          waitList.remove(abortTID);
          abortedTransactions.add(abortTID);
        }
        resumeTransactions.addAll(deadlockHandler.getTransactionsThatWaitFor(abortTID));
        deadlockHandler.clearEdge(abortTID);
        clearLocksByTID(abortTID);
      }
    }

    resumeWaitingTIDs(resumeTransactions);
    runPendingOperations();
    return true;
  }


  private boolean resumeWaitingTIDs(Set<Integer> resumeTIDs) {
    if (resumeTIDs != null && !resumeTIDs.isEmpty()) {
      for (int tempTID : resumeTIDs) {
        if (waitList.contains(tempTID)) {
          waitList.remove(waitList.indexOf(tempTID));
          System.out.println(tempTID + " resumed to runningList.");
          deadlockHandler.clearEdge(tempTID);
          runningTransactions.add(tempTID);
          this.runPendingOperations();
        }
      }
      return true;
    }
    return false;
  }

  private boolean runPendingOperations() {
    Queue<Operation> oldPending = new LinkedList<>(pendingOperations);
    this.pendingOperations.clear();
    for (Operation op : oldPending) {
      if (op.isWriteOperation()) {
        executeWriteOperation(op.getTransactionId(),
            op.getVariableId(),
            op.getVariableVal());
      }
    }
    return true;
  }

  private boolean abort(Integer abortTID, String message) {
    Set<Integer> abortTIDset = new HashSet<>();
    abortTIDset.add(abortTID);
    abort(abortTIDset, message);
    return true;
  }

}