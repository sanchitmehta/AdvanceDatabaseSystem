package ADBFinalProject;

public class Transaction {

  private final int id;
  private final int startTime;
  private int endTime;

  Transaction(int transactionId, int startTime) {
    this.id = transactionId;
    this.startTime = startTime;
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
}