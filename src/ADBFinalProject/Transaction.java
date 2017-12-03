package ADBFinalProject;

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
  }

  boolean addOperation(Operation op) {
    operations.add(op);
    System.out.println("Adding a operation to Transaction id " + id);
    if (!pendingWrite) {
      pendingWrite = op.isWriteOperation();
    }
    return true;
  }

  @Override
  public String toString() {
    return "Transaction{" +
        "id=" + id +
        '}';
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