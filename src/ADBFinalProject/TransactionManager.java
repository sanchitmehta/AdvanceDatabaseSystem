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
  private Map<Integer, Integer> varValMap;

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
    varValMap = new HashMap<>();
    setUpVarValMap();
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
    System.out.println(readOnlyTransaction.toString() + " has begun!");
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
      if (!commitTransaction(tId)) {
        runningTransactions.remove(tId);
        clearLocksByTID(tId);
        abort(tId, "Aborting Transaction{id=" + tId + "}. Commit Failed");
        return false;
      }
      Set<Integer> resumeTIDs = deadlockHandler.getTransactionsThatWaitBy(tId);
      deadlockHandler.removeTransactionEdge(tId);
      clearLocksByTID(tId);
      resumeWaitingTIDs(resumeTIDs);
      runPendingOperations();
      runningTransactions.remove(tId);
      endedTransaction.add(tId);
      //System.out.println("Transaction " + tId + " committed.");
      //System.out.println("Ending Transaction " + tId);
      //runPendingOperations();
    } else if (readOnlyRunningTransactions.contains(tId)) {
      readOnlyRunningTransactions.remove(tId);
      t = indexToTransactionMap.remove(tId);
      //t.setEndTime(TIMER);
      endedTransaction.add(tId);
    } else if (abortedTransactions.contains(tId)) {
      System.out.println("Transaction{id=" + tId + "} was aborted and thus cannot commit.");
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
    } else if (waitList.contains(tId) && !runningTransactions.isEmpty()) {
      waitList.remove(waitList.indexOf(tId));
      runningTransactions.add(tId);
      if (!commitTransaction(tId)) {
        runningTransactions.remove(tId);
        clearLocksByTID(tId);
        abort(tId, "Aborting Transaction{id=" + tId + "}. Commit Failed");
        return false;
      }
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
    if (!runOperations(indexToTransactionMap.get(tId).getPendingOperations(), tId)) {
      return false;
    }
    indexToTransactionMap.get(tId).clearPendingOperations();
    System.out.println("Transaction{id=" + tId + "} committed successfully.");
    return true;
  }

  private void setUpVarValMap() {
    for (int varIndex = 1; varIndex <= TransactionManager.NUMBER_OF_VARIABLES; varIndex++) {
      this.varValMap.put(varIndex, varIndex * 10);
    }
  }

  private boolean runOperations(List<Operation> opList, int tId) {

    boolean abortedFlag = false;
    for (Operation opTemp : opList) {
      int commitCount = 0;
      for (int i = 1; i < sites.length; i++) {
        int varIndex = opTemp.getVariableId();
        if (!sites[i].hasVariable(varIndex)) {
          continue;
        }
        if (opTemp.isWriteOperation()) {
          int newValue = opTemp.getVariableVal();
          boolean varCommitted = sites[i].commitVariableValue(varIndex, newValue, tId);
          if (varCommitted) {
            varValMap.put(varIndex, newValue);
            commitCount++;
            sites[i].clearWriteLockForVariable(varIndex);
          } else if (sites[i].isRunning()
            && sites[i].hasVariable(varIndex)
            && !varCommitted) {
            abort(tId, "Aborting Transaction{id=" + tId + "} - unable to commit " +
              "Variable{id=" + varIndex + "}");
            break;
            //break; // just abort once
          }
          //v.updateValue(newValue);
          //(newValue, varIndex, T.getTID());
        } else if (opTemp.isReadOperation()) {
          if (sites[i].isRunning()) {
            if (!sites[i].getVariableByIndex(varIndex).hasReadLockByTransction(tId)) {
              return false;
            }
            System.out.println("Transaction{id=" + tId + "} read var x"
              + varIndex + "="
              + sites[i].getVariableValue(varIndex) + " from site " + i);
            commitCount++;
            break;
          }
        }
      }
      /*
      if (commitCount == 0) {
        abort(tId, "Site is down - Commit not possible, Transaction aborted");
      }
      */
    }
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
      Integer variableValue = readOnlyTransaction.getVariableValue(vId);
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
        //System.out.println(tId + " moved to waitList.");
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

  boolean failSite(int siteId) {
    if (!sites[siteId].isRunning()) {
      return false;
    }
    Map<Integer, Integer> varLockMap = sites[siteId].getVariablesWithWriteLocks();
    Map<Integer, List<Integer>> varReadLock = sites[siteId].getAllVariablesWithReadLocks();
    sites[siteId].failCurrentSite();
    for (int vId : varLockMap.keySet()) {
      for (int i = 1; i < sites.length; i++) {
        if (sites[i].hasVariable(vId)) {
          sites[i].clearLocksOf(varLockMap.get(vId));
        }
      }
    }
    for (int vId : varReadLock.keySet()) {
      for (int i = 1; i < sites.length; i++) {
        if (sites[i].hasVariable(vId)) {
          sites[i].clearReadLockTIDsOfVariable(vId, varReadLock.get(vId));
        }
      }
    }
    System.out.println("Site{id=" + siteId + "} failed");
    return true;
  }

  boolean recoverSite(int siteId) {
    if (sites[siteId].isRunning()) {
      return false;
    }
    sites[siteId].recoverSite();
    int recoveredVarCount = 0;
    if (!sites[siteId].recoverSiteVariablesFromBackup(varValMap)) {
      System.out.println("Site{id=" + siteId + "} recovery failed.");
      return false;
    }

    for (int vIdx : sites[siteId].getIndexToVarMap().keySet()) {
      for (int i = 1; i < sites.length; i++) {
        if (i != siteId && sites[i].hasVariable(vIdx) && sites[i].isRunning()) {
          sites[siteId].updateLocksForVariable(vIdx,
            sites[i].getWriteLockTIDForVariable(vIdx),
            new ArrayList<>(sites[i].getReadLockSet(vIdx)));
          break;
        }
      }
    }
    System.out.println("Site{id=" + siteId + "} recovered");
    return true;
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
      System.out.println("Transaction{id=" + tID + "} moved to waitList.");
      runningTransactions.remove(tID);
    }
    return status;
  }

  private LockRequestStatus waitDieProtocol(int lockingTID, int requestTID) {

    Transaction requestT, lockingT;
    requestT = indexToTransactionMap.get(requestTID);
    lockingT = indexToTransactionMap.get(lockingTID);

    if (deadlockHandler.checkDeadlockForEdge(requestTID, lockingTID)
      && lockingT != null
      && lockingT.getStartTime() < requestT.getStartTime()) {
      String message = "Aborting Transaction{id=" + requestTID + "} due to deadlock.";
      abort(requestTID, message);
      return LockRequestStatus.TRANSACTION_ABORTED;
    } else if (lockingT != null) {
      deadlockHandler.addTransactionEdge(requestTID, lockingTID);
      waitList.add(requestTID);
      //System.out.println(requestTID + " moved to waitList.");
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
          waitList.remove(waitList.indexOf(abortTID));
          abortedTransactions.add(abortTID);
        }
        Set<Integer> nextLevelResumeList = deadlockHandler.getTransactionsThatWaitBy(abortTID);
        if (nextLevelResumeList != null && !nextLevelResumeList.isEmpty()) {
          resumeTransactions.addAll(nextLevelResumeList);
        }
        deadlockHandler.removeTransactionEdge(abortTID);
        clearLocksByTID(abortTID);
      }
    }
    if (runningTransactions.isEmpty()) {
      resumeWaitingTIDs(resumeTransactions);
      runPendingOperations();
    }
    return true;
  }


  private boolean resumeWaitingTIDs(Set<Integer> resumeTIDs) {
    List<Integer> tempList = new ArrayList<>(waitList);
    if (resumeTIDs != null && !resumeTIDs.isEmpty()) {
      for (int tempTID : tempList) {
        if (resumeTIDs.contains(tempTID)) {
          waitList.remove(waitList.indexOf(tempTID));
          //System.out.println(tempTID + " resumed to runningList.");
          deadlockHandler.removeTransactionEdge(tempTID);
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

  boolean hasTransaction(int tId) {
    return indexToTransactionMap.containsKey(tId);
  }

}