package simpledb.tx.concurrency;

import java.util._;
import simpledb.file.BlockId;
import collection.JavaConverters._;

/** The concurrency manager for the transaction. Each transaction has its own
  * concurrency manager. The concurrency manager keeps track of which locks the
  * transaction currently has, and interacts with the global lock table as
  * needed.
  * @author
  *   Edward Sciore
  */
class ConcurrencyMgr {

  /** The global lock table. This variable is static because all transactions
    * share the same table.
    */
  private val locktbl: LockTable = new LockTable();
  private val locks: Map[BlockId, String] =
    scala.collection.mutable.HashMap().asJava;

  /** Obtain an SLock on the block, if necessary. The method will ask the lock
    * table for an SLock if the transaction currently has no locks on that
    * block.
    * @param blk
    *   a reference to the disk block
    */
  def sLock(blk: BlockId): Unit = {
    if (locks.get(blk) == null) {
      locktbl.sLock(blk);
      locks.put(blk, "S");
    }
  }

  /** Obtain an XLock on the block, if necessary. If the transaction does not
    * have an XLock on that block, then the method first gets an SLock on that
    * block (if necessary), and then upgrades it to an XLock.
    * @param blk
    *   a reference to the disk block
    */
  def xLock(blk: BlockId): Unit = {
    if (!hasXLock(blk)) {
      sLock(blk);
      locktbl.xLock(blk);
      locks.put(blk, "X");
    }
  }

  /** Release all locks by asking the lock table to unlock each one.
    */
  def release(): Unit = {
    locks
      .keySet()
      .forEach(blk => {
        locktbl.unlock(blk);
      })
    locks.clear();
  }

  private def hasXLock(blk: BlockId): Boolean = {
    val locktype: String = locks.get(blk);
    locktype != null && locktype.equals("X");
  }
}
