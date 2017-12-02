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
public class DeadlockHandler {

  private Map<Integer, Set<Integer>> waitsByGraph;
  private Map<Integer, Integer> waitsForGraph;

  public DeadlockHandler() {
    waitsForGraph = new HashMap<>();
    waitsByGraph = new HashMap<>();
  }

  public boolean addTransactionEdge(int tID1, int tID2) {
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

  public boolean transactionsHaveCycle(int tID1, int tID2) {
    return (this.waitsForGraph.containsKey(tID2)
      && this.waitsForGraph.get(tID2).equals(tID1))
      || (this.waitsByGraph.containsKey(tID1)
      && this.waitsByGraph.get(tID1).contains(tID2));
  }
}
