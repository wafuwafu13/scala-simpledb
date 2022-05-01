package simpledb.tx.recovery

import simpledb.file.Page;
import simpledb.tx.Transaction;

/** The interface implemented by each type of log record.
  * @author
  *   Edward Sciore
  */
class DummyLogRecord {
  final val CHECKPOINT: Int = 0
  final val START: Int = 1
  final val COMMIT: Int = 2
  final val ROLLBACK: Int = 3
  final val SETINT: Int = 4
  final val SETSTRING: Int = 5

  /** Returns the log record's type.
    * @return
    *   the log record's type
    */
  def op(): Int = { -100 };

  /** Returns the transaction id stored with the log record.
    * @return
    *   the log record's transaction id
    */
  def txNumber(): Int = { -100 };

  /** Undoes the operation encoded by this log record. The only log record types
    * for which this method does anything interesting are SETINT and SETSTRING.
    * @param txnum
    *   the id of the transaction that is performing the undo.
    */
  def undo(tx: Transaction): Unit = { println("dummy") };

  /** Interpret the bytes returned by the log iterator.
    * @param bytes
    * @return
    */
  def createLogRecord(bytes: Array[Byte]) = {
    val p: Page = new Page(bytes);
    p.getInt(0) match {
      case CHECKPOINT =>
        new CheckpointRecord();
      case START =>
        new StartRecord(p);
      case COMMIT =>
        new CommitRecord(p);
      case ROLLBACK =>
        new RollbackRecord(p);
      case SETINT =>
        new SetIntRecord(p);
      case SETSTRING =>
        new SetStringRecord(p);
      case _ =>
        null;
    }
  }
}
