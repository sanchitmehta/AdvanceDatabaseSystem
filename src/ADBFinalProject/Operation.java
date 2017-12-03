package ADBFinalProject;

/**
 * @author Sanchit Mehta, Pranav Chaphekar
 * @see Site
 * @see Transaction
 * @see TransactionManager
 */
class Operation {

  private int transactionId;
  private int variableId;
  private int variableVal;
  private boolean isWriteOperation;
  private boolean isEndOperation;

  Operation(int transactionId, int variableId, int variableVal) {
    this.transactionId = transactionId;
    this.variableId = variableId;
    this.variableVal = variableVal;
    this.isWriteOperation = true;
    isEndOperation = false;
  }

  Operation(int transactionId, int variableId, boolean isWriteOperation) {
    this.transactionId = transactionId;
    this.variableId = variableId;
    this.isWriteOperation = false;
    isEndOperation = false;
  }

  Operation() {
    isEndOperation = false;
  }

  public boolean isEndOperation() {
    return isEndOperation;
  }

  public int getTransactionId() {
    return transactionId;
  }

  int getVariableId() {
    return variableId;
  }

  int getVariableVal() {
    if (!isWriteOperation) {
      throw new UnsupportedOperationException();
    }
    return variableVal;
  }

  boolean isWriteOperation() {
    return isWriteOperation;
  }

  boolean isReadOperation() {
    return !isWriteOperation && !isEndOperation;
  }

}
