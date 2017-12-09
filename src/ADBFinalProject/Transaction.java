package ADBFinalProject;

import java.util.ArrayList;
import java.util.List;

class Transaction {

  private final int id;
  private final int startTime;
  private List<Operation> operations;
  private boolean pendingWrite;

  Transaction(int transactionId, int startTime) {
    this.id = transactionId;
    this.startTime = startTime;
    operations = new ArrayList<>();
  }

  /**
   * Adds the operation to the queue
   *
   * @param op The operation
   * @return true if successful, false otherwise
   */
  boolean addOperation(Operation op) {
    operations.add(op);
    System.out.println("Adding " +
        (op.isReadOperation() ? "read" : "write")
        + " operation to Transaction{id=" + id + "}");
    if (!pendingWrite) {
      pendingWrite = op.isWriteOperation();
    }
    return true;
  }

  /**
   * Gets the list of pending operations
   *
   * @return List of pending operations
   */
  List<Operation> getPendingOperations() {
    return operations;
  }

  /**
   * Deletes all the pending operations
   */
  void clearPendingOperations() {
    operations.clear();
  }

  @Override
  public String toString() {
    return "Transaction{" +
        "id=" + id +
        '}';
  }

  /**
   * Getter method for the id of this transaction
   *
   * @return transaction Id
   */
  int getId() {
    return id;
  }

  /**
   * Getter method for the start time of the transaction
   *
   * @return time when the transaction has started
   */
  int getStartTime() {
    return startTime;
  }
}