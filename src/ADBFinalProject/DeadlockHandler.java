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
    if (transactionsHaveCycle(tID1, tID2) || pathExistsBetween(tID2, tID1, tID2)) {
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

  private boolean pathExistsBetween(int startTNode, int endTNode, int curTNode) {
    if (curTNode == endTNode) {
      return true;
    }
    if (waitsForGraph.get(curTNode) != null) {
      return pathExistsBetween(startTNode, endTNode, waitsForGraph.get(curTNode));
    }
    return false;
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

  boolean transactionsHaveCycle(int tID1, int tID2) {
    return (this.waitsForGraph.containsKey(tID2)
      && this.waitsForGraph.get(tID2).equals(tID1))
      || (this.waitsByGraph.containsKey(tID1)
      && this.waitsByGraph.get(tID1).contains(tID2)
      || this.pathExistsBetween(tID2, tID1, tID2));
  }

  Set<Integer> getTransactionsThatWaitBy(int tId) {
    return waitsByGraph.get(tId);
  }

  boolean clearEdge(int targetTID) {
    Set<Integer> waitsForKeys = new HashSet<>(this.waitsForGraph.keySet());
    if (this.waitsForGraph.containsKey(targetTID)) {
      for (int olderTID : waitsForKeys) {
        //if (this.waitsForGraph.get(olderTID).equals(targetTID)) {
        if (olderTID == targetTID) {
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
