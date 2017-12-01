package ADBFinalProject;

import java.util.Map;

public class TransactionManager {

  private Map<Integer, Transaction> runningTransactions;

  public void addTransaction(Integer tId, Transaction transaction) {
    runningTransactions.put(tId, transaction);
  }
}