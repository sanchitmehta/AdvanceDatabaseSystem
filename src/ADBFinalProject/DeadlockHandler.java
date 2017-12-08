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

  private Map<Integer, Integer> waitsForGraph;
  private Map<Integer, Set<Integer>> waitsByGraph;

  DeadlockHandler() {
    waitsForGraph = new HashMap<>();
    waitsByGraph = new HashMap<>();
  }

  /**
   * Adds an edge to the Waits for Graph and
   * Waits by Graph
   *
   * @param tID1 start transaction Id
   * @param tID2 end transaction Id
   * @return true if everything is successful; false otherwise
   */
  boolean addTransactionEdge(int tID1, int tID2) {
    if (pathExistsBetween(tID2, tID1)
        || checkDeadlockForEdge(tID1, tID2)) {
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

  /**
   * Removes an edge in the waitsForGraph/waitsByGraph, typically
   * called when a transaction commits/aborts
   *
   * @param tID remove all edges from and to this transation
   * @return true is remove operation was successful
   */
  boolean removeTransactionEdge(int tID) {
    Set<Integer> waitsForKeys = new HashSet<>(this.waitsForGraph.keySet());
    if (this.waitsForGraph.containsKey(tID)) {
      for (int olderTID : waitsForKeys) {
        if (olderTID == tID) {
          this.waitsForGraph.remove(olderTID);
        }
      }
      if (this.waitsByGraph.containsKey(tID)) {
        this.waitsByGraph.remove(tID);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * This method is used to detect a deadlock pre-emptively
   *
   * @return true if an addition of between transactions startTiD and endTiD would result in a
   * deadlock
   */
  boolean checkDeadlockForEdge(int startTID, int endTID) {
    return (this.waitsForGraph.containsKey(endTID)
        && this.waitsForGraph.get(endTID).equals(startTID))
        || (this.waitsByGraph.containsKey(startTID)
        && this.waitsByGraph.get(startTID).contains(endTID)
        || this.pathExistsBetween(endTID, startTID));
  }

  /**
   * Returns a set of transactions that are waiting
   * for a transaction to complete
   *
   * @param tId : transaction id for which other transactions wait
   * @return : Set of transactions waiting for this transaction
   */
  Set<Integer> getTransactionsThatWaitBy(int tId) {
    return waitsByGraph.get(tId);
  }

  /**
   * Uses DFS to to detect if there is a path between two nodes
   *
   * @param endTiD end transaction id for dfs
   * @param curTiD start transaction id for dfs
   * @return true if there is path between the two nodes
   */
  private boolean pathExistsBetween(int curTiD, int endTiD) {
    return curTiD == endTiD
        || waitsForGraph.get(curTiD) != null
        && pathExistsBetween(waitsForGraph.get(curTiD), endTiD);
  }
}
