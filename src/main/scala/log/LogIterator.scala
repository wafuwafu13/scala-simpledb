package simpledb.log;

import java.util.Iterator;
import simpledb.file._;

/** A class that provides the ability to move through the records of the log
  * file in reverse order.
  *
  * @author
  *   Edward Sciore
  */
class LogIterator(val fm: FileMgr, var blk: BlockId)
    extends Iterator[Array[Byte]] {

  /** Creates an iterator for the records in the log file, positioned after the
    * last log record.
    */
  val b: Array[Byte] = new Array[Byte](fm.blocksize);
  protected var p = new Page(b);

  protected var currentpos: Int = 0;
  protected var boundary: Int = 0;

  moveToBlock(blk);

  /** Determines if the current log record is the earliest record in the log
    * file.
    * @return
    *   true if there is an earlier record
    */
  def hasNext(): Boolean = {
    return currentpos < fm.blockSize() || blk.number() > 0;
  }

  /** Moves to the next log record in the block. If there are no more log
    * records in the block, then move to the previous block and return the log
    * record from there.
    * @return
    *   the next earliest log record
    */
  def next(): Array[Byte] = {
    if (currentpos == fm.blockSize()) {
      blk = new BlockId(blk.fileName(), blk.number() - 1);
      moveToBlock(blk);
    }
    val rec: Array[Byte] = p.getBytes(currentpos);
    currentpos += Integer.BYTES + rec.length;
    return rec;
  }

  /** Moves to the specified log block and positions it at the first record in
    * that block (i.e., the most recent one).
    */
  private def moveToBlock(blk: BlockId): Unit = {
    fm.read(blk, p);
    boundary = p.getInt(0);
    currentpos = boundary;
  }
}
