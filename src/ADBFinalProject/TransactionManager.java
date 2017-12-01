package ADBFinalProject;

import java.util.HashMap;
import java.util.Map;

class TransactionManager {

  private Map<Integer, Transaction> runningTransactions;

  TransactionManager() {
    this.runningTransactions = new HashMap<>();
  }

  /**
   * Adds the transaction to the running Transactions map to keep a track of currently running
   * transactions
   *
   * @param tId transaction Id
   * @param transaction {@code Transaction}
   */
  void addTransaction(Integer tId, Transaction transaction) {
    runningTransactions.put(tId, transaction);
  }

  /**
   * Deletes the transaction from the running Transaction map as soon as the transaction is ended
   *
   * @param tId transaction Id
   * @return true if deletion is successful, false otherwise
   */
  boolean deleteTransaction(Integer tId) {
    if (runningTransactions.containsKey(tId)) {
      runningTransactions.remove(tId);
      return true;
    } else {
      return false;
    }
  }
}