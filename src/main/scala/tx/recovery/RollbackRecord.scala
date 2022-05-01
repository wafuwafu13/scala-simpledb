package simpledb.tx.recovery;

import simpledb.file._;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

/** The ROLLBACK log record.
  * @author
  *   Edward Sciore
  */
class RollbackRecord(val p: Any) extends DummyLogRecord {

  /** Create a RollbackRecord object.
    * @param txnum
    *   the ID of the specified transaction
    */
  private val tpos: Int = Integer.BYTES;
  private val txnum: Int =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getInt(tpos) else 0;

  override def op(): Int = {
    ROLLBACK;
  }

  override def txNumber(): Int = {
    txnum;
  }

  override def toString(): String = {
    "<ROLLBACK " + txnum + ">";
  }

  /** Does nothing, because a rollback record contains no undo information.
    */
  override def undo(tx: Transaction): Unit = {}

  /** A static method to write a rollback record to the log. This log record
    * contains the ROLLBACK operator, followed by the transaction id.
    * @return
    *   the LSN of the last log value
    */
  def writeToLog(
      lm: LogMgr,
      txnum: Int
  ): Int = {
    val rec: Array[Byte] = new Array[Byte](2 * Integer.BYTES);
    val p: Page = new Page(rec);
    p.setInt(0, ROLLBACK);
    p.setInt(Integer.BYTES, txnum);
    lm.append(rec);
  }
}
