package simpledb.tx.concurrency;

import java.util._;
import simpledb.file.BlockId;
import collection.JavaConverters._;

/** The lock table, which provides methods to lock and unlock blocks. If a
  * transaction requests a lock that causes a conflict with an existing lock,
  * then that transaction is placed on a wait list. There is only one wait list
  * for all blocks. When the last lock on a block is unlocked, then all
  * transactions are removed from the wait list and rescheduled. If one of those
  * transactions discovers that the lock it is waiting for is still locked, it
  * will place itself back on the wait list.
  * @author
  *   Edward Sciore
  */
class LockTable {
  private final val MAX_TIME: Long = 10000; // 10 seconds

  private val locks: Map[BlockId, Integer] =
    scala.collection.mutable.HashMap().asJava;

  /** Grant an SLock on the specified block. If an XLock exists when the method
    * is called, then the calling thread will be placed on a wait list until the
    * lock is released. If the thread remains on the wait list for a certain
    * amount of time (currently 10 seconds), then an exception is thrown.
    * @param blk
    *   a reference to the disk block
    */
  def sLock(blk: BlockId) = synchronized {
    try {
      val timestamp: Long = System.currentTimeMillis();
      while (hasXlock(blk) && !waitingTooLong(timestamp))
        wait(MAX_TIME);
      if (hasXlock(blk))
        throw new LockAbortException();
      val value: Int = getLockVal(blk); // will not be negative
      locks.put(blk, value + 1);
    } catch {
      case e: InterruptedException => throw new LockAbortException();
    }
  }

  /** Grant an XLock on the specified block. If a lock of any type exists when
    * the method is called, then the calling thread will be placed on a wait
    * list until the locks are released. If the thread remains on the wait list
    * for a certain amount of time (currently 10 seconds), then an exception is
    * thrown.
    * @param blk
    *   a reference to the disk block
    */
  def xLock(blk: BlockId) = synchronized {
    try {
      val timestamp: Long = System.currentTimeMillis();
      while (hasOtherSLocks(blk) && !waitingTooLong(timestamp))
        wait(MAX_TIME);
      if (hasOtherSLocks(blk))
        throw new LockAbortException();
      locks.put(blk, -1);
    } catch {
      case e: InterruptedException => throw new LockAbortException();
    }
  }

  /** Release a lock on the specified block. If this lock is the last lock on
    * that block, then the waiting transactions are notified.
    * @param blk
    *   a reference to the disk block
    */
  def unlock(blk: BlockId) = synchronized {
    val value: Int = getLockVal(blk);
    if (value > 1)
      locks.put(blk, value - 1);
    else {
      locks.remove(blk);
      notifyAll();
    }
  }

  private def hasXlock(blk: BlockId): Boolean = {
    getLockVal(blk) < 0;
  }

  private def hasOtherSLocks(blk: BlockId): Boolean = {
    getLockVal(blk) > 1;
  }

  private def waitingTooLong(starttime: Long): Boolean = {
    System.currentTimeMillis() - starttime > MAX_TIME;
  }

  private def getLockVal(blk: BlockId): Int = {
    val ival: Integer = locks.get(blk);
    if (ival == null) 0 else ival.intValue();
  }
}
