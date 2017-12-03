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

  Operation(int transactionId, int variableId, int variableVal, boolean isWriteOperation) {
    this.transactionId = transactionId;
    this.variableId = variableId;
    this.variableVal = variableVal;
    this.isWriteOperation = isWriteOperation;
  }

  public int getTransactionId() {
    return transactionId;
  }

  public int getVariableId() {
    return variableId;
  }

  public int getVariableVal() {
    return variableVal;
  }

  boolean isWriteOperation() {
    return isWriteOperation;
  }

}
