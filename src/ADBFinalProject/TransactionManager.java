package ADBFinalProject;

import java.util.Map;

class TransactionManager {

  private Map<Integer, Transaction> runningTransactions;

  void addTransaction(Integer tId, Transaction transaction) {
    runningTransactions.put(tId, transaction);
  }

  boolean deleteTransaction(Integer tId) {
    if (runningTransactions.containsKey(tId)) {
      runningTransactions.remove(tId);
      return true;
    } else {
      return false;
    }
  }
}