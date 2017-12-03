package ADBFinalProject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Removes a deadlock in transactions
 * waits for graph by removing the youngest
 * transaction
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 * @see Site
 * @see Transaction
 * @see TransactionManager
 */
class DeadlockHandler {

  private Map<Integer, Set<Integer>> waitsByGraph;
  private Map<Integer, Integer> waitsForGraph;

  DeadlockHandler() {
    waitsForGraph = new HashMap<>();
    waitsByGraph = new HashMap<>();
  }

  boolean addTransactionEdge(int tID1, int tID2) {
    if (transactionsHaveCycle(tID1, tID2)) {
      return false;
    } else if (waitsForGraph.containsKey(tID1)
      && waitsForGraph.get(tID1) == tID2) {
      return false;
    }
    waitsForGraph.put(tID1, tID2);
    waitsByGraph.putIfAbsent(tID2, new HashSet<>());
    Set<Integer> waitingTransactions = waitsByGraph.get(tID2);
    waitingTransactions.add(tID1);
    return true;
  }

  public boolean removeTransactionEdge(int tID1, int tID2) {
    if (waitsForGraph.containsKey(tID1)
      && waitsForGraph.get(tID1) == tID2) {
      waitsForGraph.remove(tID1);
      waitsByGraph.get(tID2).remove(tID1);
      return true;
    }
    return false;
  }

  private boolean transactionsHaveCycle(int tID1, int tID2) {
    return (this.waitsForGraph.containsKey(tID2)
      && this.waitsForGraph.get(tID2).equals(tID1))
      || (this.waitsByGraph.containsKey(tID1)
      && this.waitsByGraph.get(tID1).contains(tID2));
  }

  Set<Integer> getTransactionsThatWaitFor(int tId) {
    return waitsByGraph.get(tId);
  }

  boolean clearEdge(int targetTID) {
    if (this.waitsForGraph.containsKey(targetTID)) {
      for (int olderTID : this.waitsForGraph.keySet()) {
        if (this.waitsForGraph.get(olderTID).equals(targetTID)) {
          this.waitsForGraph.remove(olderTID);
        }
      }
      if (this.waitsByGraph.containsKey(targetTID)) {
        this.waitsByGraph.remove(targetTID);
      }
      return true;
    } else {
      return false;
    }
  }
}
