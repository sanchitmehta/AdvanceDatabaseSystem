package ADBFinalProject;

/**
 * Contains the details of the operation (operation can be read or write)
 *
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
    this.isEndOperation = false;
  }

  Operation(int transactionId, int variableId, boolean isWriteOperation) {
    this.transactionId = transactionId;
    this.variableId = variableId;
    this.isWriteOperation = isWriteOperation;
    this.isEndOperation = false;
  }

  Operation() {
    isEndOperation = false;
  }

  /**
   * Checks if the operation is a write operation
   *
   * @return true if it is a write operation; false otherwise
   */
  boolean isWriteOperation() {
    return isWriteOperation;
  }

  /**
   * Sets the flag for the read operation
   *
   * @return true if the operation is a read operation, false otherwise
   */
  boolean isReadOperation() {
    return !isWriteOperation && !isEndOperation;
  }

  /**
   * Getter method for transaction Id
   *
   * @return transaction Id
   */
  int getTransactionId() {
    return transactionId;
  }

  /**
   * Getter method for variable Id
   *
   * @return variable Id
   */
  int getVariableId() {
    return variableId;
  }

  /**
   * Getter method for the value of the variable
   *
   * @return variable value
   */
  int getVariableVal() {
    if (!isWriteOperation) {
      throw new UnsupportedOperationException();
    }
    return variableVal;
  }

  @Override
  public String toString() {
    if (this.isWriteOperation) {

      return "Operation{" +
          "transactionId=" + transactionId +
          ", variableId=" + variableId +
          ", variableVal=" + variableVal +
          "is a write operation";
    } else {
      return "Operation{" +
          "transactionId=" + transactionId +
          ", variableId=" + variableId +
          ", variableVal=" + variableVal +
          ", is a read operation";
    }
  }
}