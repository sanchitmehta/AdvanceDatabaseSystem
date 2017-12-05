package ADBFinalProject;

import java.util.*;

class TransactionManager {

  static final int NUMBER_OF_VARIABLES = 20;
  private static final int NUMBER_OF_SITES = 10;
  private Map<Integer, Transaction> indexToTransactionMap;
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
   * @param tId         transaction Id
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
   * @param tId                 transaction Id
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
      //t.setEndTime(TIMER);
      commitTransaction(tId);
      Set<Integer> resumeTIDs = deadlockHandler.getTransactionsThatWaitBy(tId);
      deadlockHandler.clearEdge(tId);
      clearLocksByTID(tId);
      resumeWaitingTIDs(resumeTIDs);
      runPendingOperations();
      runningTransactions.remove(tId);
      endedTransaction.add(tId);
      //System.out.println("Ending Transaction " + tId);
      //runPendingOperations();
    } else if (readOnlyRunningTransactions.contains(tId)) {
      readOnlyRunningTransactions.remove(tId);
      t = indexToTransactionMap.remove(tId);
      //t.setEndTime(TIMER);
      endedTransaction.add(tId);
    } else if (abortedTransactions.contains(tId)) {
      System.out.println(tId + " was aborted and thus cannot commit.");
    } else if (runningTransactions.isEmpty() && waitList.contains(tId)) {
      t = indexToTransactionMap.get(tId);
      waitList.remove(waitList.indexOf(tId));
      runningTransactions.add(tId);
      commitTransaction(tId);
      clearLocksByTID(tId);
      runPendingOperations();
      runOperations(indexToTransactionMap.get(tId).getPendingOperations(), tId);
      runningTransactions.remove(tId);
      endedTransaction.add(tId);
    } else {
      addPendingOperation(new Operation());
    }
    return true;
  }

  private boolean commitTransaction(int tId) {
    //sites[i].executeTransaction(tId);
    runOperations(indexToTransactionMap.get(tId).getPendingOperations(), tId);
    indexToTransactionMap.get(tId).clearPendingOperations();
    System.out.println("Transaction " + tId + " committed.");
    return true;
  }

  private void runOperations(List<Operation> opList, int tId) {

    for (Operation opTemp : opList) {
      for (int i = 1; i < sites.length; i++) {
        int varIndex = opTemp.getVariableId();
        if (opTemp.isWriteOperation() && sites[i].hasVariable(varIndex)) {
          int newValue = opTemp.getVariableVal();
          sites[i].commitVariableValue(varIndex, newValue);
          //v.updateValue(newValue);
          //(newValue, varIndex, T.getTID());
          sites[i].clearLocksOf(tId);
        } else if (opTemp.isReadOperation()) {
          if (sites[i].hasVariable(varIndex)) {
            System.out.println("Transaction " + tId + " read var x"
              + varIndex + "="
              + sites[i].getVariableValue(varIndex) + " from site " + i);
            break;
          }
        }
      }
    }
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
  private boolean isReadOnlyTransRunning(int tId) {
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
        //indexToTransactionMap.get(tID).addOperation(new Operation(tID, vID, varVal));
        break;
      case ALL_SITES_DOWN:
        addPendingOperation(op);
        break;
    }
    return true;
  }

  boolean executeReadOperation(int tId, int vId) {

    // if transaction has been already aborted then do not do anything
    if (this.isAbortedTransaction(tId)) {
      System.out.println("The Transaction T" + tId + "has already been aborted");
    }
    // if the transaction is a Read Only Transaction, then get the value and print it
    if (this.isReadOnlyTransRunning(tId)) {
      ReadOnlyTransaction readOnlyTransaction =
        (ReadOnlyTransaction) this.indexToTransactionMap
          .get(tId);
      Integer variableValue = readOnlyTransaction.getVariableValue(tId);
      if (variableValue != null) {
        System.out.println(readOnlyTransaction.toString() + " reads x" + vId
          + "=" + variableValue);
      } else {
        System.out.println(readOnlyTransaction.toString() + " can't read x"
          + vId + " because it is not available ");
      }
    } else if (runningTransactions.contains(tId)) {
      if (canReadFromSite(vId)) {
        int WLTID = getWriteLockTID(vId);
        if (WLTID == -1) {
          readFromSite(vId, tId);
          indexToTransactionMap.get(tId).addOperation(new Operation(tId, vId, false));
        } else if (WLTID == tId) {
          indexToTransactionMap.get(tId).addOperation(new Operation(tId, vId, false));
          //addPendingOperation(new Operation(tId,vId,false));
//          System.out.println(tId + " reads x" + vId
//            + " from its previous write, value="
//            + getDirtyWrite(tId, vId));
        } else {
          LockRequestStatus waitResult = waitDieProtocol(WLTID, tId);
          if (waitResult == LockRequestStatus.TRANSACTION_WAIT_LISTED) {
            addPendingOperation(new Operation(tId, vId, false));
          } else if (waitResult == LockRequestStatus.TRANSACTION_ABORTED) {
            this.abort(tId, "Abort " + tId + "due to wait die ");
          }
        }
      } else {
        runningTransactions.remove(tId);
        System.out.println(tId + " moved to waitList.");
        this.addPendingOperation(new Operation(tId, vId, false));
        waitList.add(tId);
      }
    }
    return true;
  }

  private boolean canReadFromSite(int varIndex) {
    for (int i = 1; i < sites.length; i++) {
      if (sites[i].hasVariable(varIndex)
        && sites[i].isVarReadable(varIndex)) {
        return true;
      }
    }
    return false;
  }

  private int readFromSite(int vId, int tId) {
    for (int i = 1; i < sites.length; i++) {
      if (sites[i].hasVariable(vId)) {
        return sites[i].getVariableValueWithReadLock(vId, tId);
      }
    }
    return -1;
  }

  private boolean updateWriteLocks(int vId, int tID) {
    for (int i = 1; i < sites.length; i++) {
      sites[i].updateWriteLock(vId, tID);
    }
    return true;
  }

  private int getWriteLockTID(int vId) {
    int lockedTID = -1;
    for (int i = 1; i < sites.length; i++) {
      if (sites[i].hasVariable(vId) && sites[i].isRunning()) {
        lockedTID = sites[i].getWriteLockTIDForVariable(vId);
      }
    }
    return lockedTID;
  }

  private int getDirtyWrite(int tId, int vId) {
    int result = Integer.MIN_VALUE;
    Transaction t;
    if (runningTransactions.contains(tId)) {
      t = indexToTransactionMap.get(tId);
      result = t.getLastWrite(vId);
    }
    return result;
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

    if (deadlockHandler.transactionsHaveCycle(requestTID, lockingTID)
      && lockingT != null
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
        Set<Integer> nextLevelResumeList = deadlockHandler.getTransactionsThatWaitBy(abortTID);
        if (nextLevelResumeList != null && !nextLevelResumeList.isEmpty()) {
          resumeTransactions.addAll(nextLevelResumeList);
        }
        deadlockHandler.clearEdge(abortTID);
        clearLocksByTID(abortTID);
      }
    }

    resumeWaitingTIDs(resumeTransactions);
    runPendingOperations();
    return true;
  }


  private boolean resumeWaitingTIDs(Set<Integer> resumeTIDs) {
    List<Integer> tempList = new ArrayList<>(waitList);
    if (resumeTIDs != null && !resumeTIDs.isEmpty()) {
      for (int tempTID : tempList) {
        if (resumeTIDs.contains(tempTID)) {
          waitList.remove(waitList.indexOf(tempTID));
          System.out.println(tempTID + " resumed to runningList.");
          deadlockHandler.clearEdge(tempTID);
          runningTransactions.add(tempTID);
          this.runPendingOperations();
          //break;
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
      } else if (op.isReadOperation()) {
        executeReadOperation(op.getTransactionId(), op.getVariableId());
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