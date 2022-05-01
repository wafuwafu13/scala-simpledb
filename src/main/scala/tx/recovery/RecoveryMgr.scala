package simpledb.tx.recovery;

import java.util._;
import simpledb.file._;
import simpledb.log._;
import simpledb.buffer._;
import simpledb.tx.Transaction;
import collection.JavaConverters._

/** The recovery manager. Each transaction has its own recovery manager.
  * @author
  *   Edward Sciore
  */
class RecoveryMgr(
    val tx: Transaction,
    val txnum: Int,
    val lm: LogMgr,
    val bm: BufferMgr
) extends DummyLogRecord {

  /** Create a recovery manager for the specified transaction.
    * @param txnum
    *   the ID of the specified transaction
    */
  val startrecord = new StartRecord(null);
  startrecord.writeToLog(lm, txnum);

  /** Write a commit record to the log, and flushes it to disk.
    */
  def commit(): Unit = {
    bm.flushAll(txnum);
    val commitrecord = new CommitRecord(null);
    val lsn: Int = commitrecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  /** Write a rollback record to the log and flush it to disk.
    */
  def rollback(): Unit = {
    doRollback();
    bm.flushAll(txnum);
    val rollbackrecord = new RollbackRecord(null);
    val lsn: Int = rollbackrecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  /** Recover uncompleted transactions from the log and then write a quiescent
    * checkpoint record to the log and flush it.
    */
  def recover(): Unit = {
    doRecover();
    bm.flushAll(txnum);
    val checkpointrecord = new CheckpointRecord();
    val lsn: Int = checkpointrecord.writeToLog(lm);
    lm.flush(lsn);
  }

  /** Write a setint record to the log and return its lsn.
    * @param buff
    *   the buffer containing the page
    * @param offset
    *   the offset of the value in the page
    * @param newval
    *   the value to be written
    */
  def setInt(buff: Buffer, offset: Int, newval: Int): Int = {
    val oldval: Int = buff.getContents().getInt(offset);
    val blk: BlockId = buff.block();
    val setintrecord = new SetIntRecord(null);
    setintrecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  /** Write a setstring record to the log and return its lsn.
    * @param buff
    *   the buffer containing the page
    * @param offset
    *   the offset of the value in the page
    * @param newval
    *   the value to be written
    */
  def setString(buff: Buffer, offset: Int, newval: String): Int = {
    val oldval: String = buff.getContents().getString(offset);
    val blk: BlockId = buff.block();
    val setstringrecord = new SetStringRecord(null);
    setstringrecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  /** Rollback the transaction, by iterating through the log records until it
    * finds the transaction's START record, calling undo() for each of the
    * transaction's log records.
    */
  private def doRollback(): Unit = {
    val iter: Iterator[Array[Byte]] = lm.iterator();
    while (iter.hasNext()) {
      val bytes: Array[Byte] = iter.next();
      val rec = new DummyLogRecord()
        .createLogRecord(bytes)
        .asInstanceOf[DummyLogRecord];
      if (rec.txNumber() == txnum) {
        if (rec.op() == START)
          return;
        rec.undo(tx);
      }
    }
  }

  /** Do a complete database recovery. The method iterates through the log
    * records. Whenever it finds a log record for an unfinished transaction, it
    * calls undo() on that record. The method stops when it encounters a
    * CHECKPOINT record or the end of the log.
    */
  private def doRecover(): Unit = {
    val finishedTxs: java.util.List[Int] = new java.util.ArrayList[Int]()
    val iter: Iterator[Array[Byte]] = lm.iterator();
    while (iter.hasNext()) {
      val bytes: Array[Byte] = iter.next();
      val rec = new DummyLogRecord()
        .createLogRecord(bytes)
        .asInstanceOf[DummyLogRecord];
      if (rec.op() == CHECKPOINT)
        return;
      if (rec.op() == COMMIT || rec.op() == ROLLBACK)
        finishedTxs.add(rec.txNumber());
      else if (!finishedTxs.contains(rec.txNumber()))
        rec.undo(tx);
    }
  }
}
