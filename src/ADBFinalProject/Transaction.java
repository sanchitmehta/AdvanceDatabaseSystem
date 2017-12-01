package ADBFinalProject;

public class Transaction {

  private final int id;
  private final int startTime;
  private int endTime;

  public Transaction(int transactionId, int startTime) {
    this.id = transactionId;
    this.startTime = startTime;
  }


}
