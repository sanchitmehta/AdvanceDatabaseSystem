package ADBFinalProject;

import java.util.HashMap;
import java.util.Map;

class TransactionManager {

  private Map<Integer, Transaction> runningTransactions;
  private Map<Integer, Transaction> runningReadOnlyTransactions;
  private Site[] sites;
  private static final int NUMBER_OF_SITES = 10;

  TransactionManager() {
    this.runningTransactions = new HashMap<>();
    this.runningReadOnlyTransactions = new HashMap<>();
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
    runningTransactions.put(tId, transaction);
  }

  /**
   * Adds the transaction to the Read Only Transactions map to keep a track of currently running
   * transactions
   *
   * @param tId transaction Id
   * @param readOnlyTransaction {@code Transaction}
   */
  void addReadOnlyTransaction(Integer tId, ReadOnlyTransaction readOnlyTransaction) {
    runningReadOnlyTransactions.put(tId, readOnlyTransaction);
  }

  /**
   * Deletes the transaction from the running Transaction map as soon as the transaction is ended
   *
   * @param tId transaction Id
   * @return true if deletion is successful, false otherwise
   */
  boolean deleteTransaction(int tId) {
    if (runningTransactions.containsKey(tId)) {
      runningTransactions.remove(tId);
      Transaction transaction = runningTransactions.get(tId);
      return transaction.endTransaction();
    } else {
      return false;
    }
  }

  /**
   * @return Array of sites
   */
  Site[] getSites() {
    return sites;
  }
}