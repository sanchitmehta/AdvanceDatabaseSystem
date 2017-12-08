package ADBFinalProject;

/**
 * Different states that transaction can have
 *
 * @author Sanchit Mehta, Pranav Chaphekar
 * @see Site
 * @see Transaction
 * @see TransactionManager
 */
enum LockRequestStatus {
  TRANSACTION_CAN_GET_LOCK,
  TRANSACTION_WAIT_LISTED,
  TRANSACTION_ABORTED,
  ALL_SITES_DOWN
}
