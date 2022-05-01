package simpledb.tx.recovery;

import simpledb.file._;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;
import java.nio.charset._;

class SetIntRecord(val p: Any) extends DummyLogRecord {

  /** Create a new setint log record.
    * @param bb
    *   the bytebuffer containing the log values
    */
  private val tpos: Int = Integer.BYTES;
  private val txnum: Int =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getInt(tpos) else 0;
  val fpos: Int = tpos + Integer.BYTES;
  val filename: String =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getString(fpos) else "";
  val bpos: Int = fpos + maxLength(filename.length());
  val blknum: Int =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getInt(bpos) else 0;
  private val blk: BlockId = new BlockId(filename, blknum);
  val opos: Int = bpos + Integer.BYTES;
  private val offset: Int =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getInt(opos) else 0;
  val vpos: Int = opos + Integer.BYTES;
  private val value: Int =
    if (p.isInstanceOf[Page]) p.asInstanceOf[Page].getInt(vpos) else 0;

  override def op(): Int = {
    SETINT;
  }

  override def txNumber(): Int = {
    txnum;
  }

  override def toString(): String = {
    "<SETSINT " + txnum + " " + blk + " " + offset + " " + value + ">";
  }

  /** Replace the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block, calls setInt to restore
    * the saved value, and unpins the buffer.
    * @see
    *   simpledb.tx.recovery.LogRecord#undo(int)
    */
  override def undo(tx: Transaction): Unit = {
    tx.pin(blk);
    tx.setInt(blk, offset, value, false); // don't log the undo!
    tx.unpin(blk);
  }

  /** A static method to write a setInt record to the log. This log record
    * contains the SETINT operator, followed by the transaction id, the
    * filename, number, and offset of the modified block, and the previous
    * integer value at that offset.
    * @return
    *   the LSN of the last log value
    */
  def writeToLog(
      lm: LogMgr,
      txnum: Int,
      blk: BlockId,
      offset: Int,
      value: Int
  ): Int = {
    val tpos: Int = Integer.BYTES;
    val fpos: Int = tpos + Integer.BYTES;
    val bpos: Int = fpos + maxLength(blk.fileName().length());
    val opos: Int = bpos + Integer.BYTES;
    val vpos: Int = opos + Integer.BYTES;
    val rec: Array[Byte] = new Array[Byte](vpos + Integer.BYTES);
    val p: Page = new Page(rec);
    p.setInt(0, SETINT);
    p.setInt(tpos, txnum);
    p.setString(fpos, blk.fileName());
    p.setInt(bpos, blk.number());
    p.setInt(opos, offset);
    p.setInt(vpos, value);
    lm.append(rec);
  }

  def maxLength(strlen: Int): Int = {
    val bytesPerChar: Float =
      StandardCharsets.US_ASCII.newEncoder().maxBytesPerChar();
    Integer.BYTES + (strlen * bytesPerChar.asInstanceOf[Int]);
  }
}
