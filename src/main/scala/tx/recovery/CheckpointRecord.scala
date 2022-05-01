package simpledb.tx.recovery;

import simpledb.file._;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

/** The CHECKPOINT log record.
  * @author
  *   Edward Sciore
  */
class CheckpointRecord extends DummyLogRecord {
  override def op(): Int = {
    CHECKPOINT;
  }

  /** Checkpoint records have no associated transaction, and so the method
    * returns a "dummy", negative txid.
    */
  override def txNumber(): Int = {
    -1; // dummy value
  }

  override def toString(): String = {
    "<CHECKPOINT>";
  }

  /** Does nothing, because a checkpoint record contains no undo information.
    */
  override def undo(tx: Transaction): Unit = {}

  /** A static method to write a checkpoint record to the log. This log record
    * contains the CHECKPOINT operator, and nothing else.
    * @return
    *   the LSN of the last log value
    */
  def writeToLog(
      lm: LogMgr
  ): Int = {
    val rec: Array[Byte] = new Array[Byte](Integer.BYTES);
    val p: Page = new Page(rec);
    p.setInt(0, CHECKPOINT);
    lm.append(rec);
  }
}
