package ADBFinalProject;

import java.util.ArrayList;
import java.util.List;

class Transaction {

  private final int id;
  private final int startTime;
  private int endTime;
  private List<Operation> operations;
  private boolean pendingWrite;

  Transaction(int transactionId, int startTime) {
    this.id = transactionId;
    this.startTime = startTime;
    operations = new ArrayList<>();
  }

  boolean addOperation(Operation op) {
    operations.add(op);
    System.out.println("Adding a buffered " +
      (op.isReadOperation() ? "read" : "write")
      + " operation to Transaction{id=" + id + "}");
    if (!pendingWrite) {
      pendingWrite = op.isWriteOperation();
    }
    return true;
  }

  List<Operation> getPendingOperations() {
    return operations;
  }

  void clearPendingOperations() {
    operations.clear();
  }

  @Override
  public String toString() {
    return "Transaction{" +
        "id=" + id +
        '}';
  }

  int getLastWrite(int varIndex) {
    int result = Integer.MIN_VALUE;
    for (Operation op : operations) {
      if (!op.isReadOperation() && op.getVariableId() == varIndex) {
        result = op.getVariableVal();
      }
    }
    return result;
  }

  /**
   * Ends this transaction
   *
   * @return true if transaction has been terminated successfully, false otherwise
   */
  boolean endTransaction() {
    // To be implemented - (clean up processes)
    System.out.println(this.toString() + " has ended");
    return true;
  }

  int getId() {
    return id;
  }

  int getStartTime() {
    return startTime;
  }

  public int getEndTime() {
    return endTime;
  }
}