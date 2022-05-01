package simpledb.tx.recovery;

import simpledb.file._;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

/** The COMMIT log record
  * @author
  *   Edward Sciore
  */
class StartRecord(val p: Any) extends DummyLogRecord {
  private val tpos: Int = Integer.BYTES;
  private val txnum: Int =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getInt(tpos) else 0;

  override def op(): Int = {
    START;
  }

  override def txNumber(): Int = {
    txnum;
  }

  override def toString(): String = {
    "<START " + txnum + ">";
  }

  /** Does nothing, because a commit record contains no undo information.
    */
  override def undo(tx: Transaction): Unit = {}

  /** A static method to write a commit record to the log. This log record
    * contains the COMMIT operator, followed by the transaction id.
    * @return
    *   the LSN of the last log value
    */
  def writeToLog(
      lm: LogMgr,
      txnum: Int
  ): Int = {
    val rec: Array[Byte] = new Array[Byte](2 * Integer.BYTES);
    val p: Page = new Page(rec);
    p.setInt(0, START);
    p.setInt(Integer.BYTES, txnum);
    lm.append(rec);
  }
}
