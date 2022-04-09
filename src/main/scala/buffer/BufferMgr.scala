package simpledb.buffer;

import simpledb.file._;
import simpledb.log.LogMgr;

/** Manages the pinning and unpinning of buffers to blocks.
  * @author
  *   Edward Sciore
  */
class BufferMgr(fm: FileMgr, lm: LogMgr, numbuffs: Int) {

  /** Creates a buffer manager having the specified number of buffer slots. This
    * constructor depends on a {@link FileMgr} and {@link simpledb.log.LogMgr
    * LogMgr} object.
    * @param numbuffs
    *   the number of buffer slots to allocate
    */
  private val bufferpool: Array[Buffer] = new Array[Buffer](numbuffs);
  private var numAvailable: Int = numbuffs;
  private final val MAX_TIME: Long = 10000; // 10 seconds
  for (i <- 0 until numbuffs) {
    bufferpool(i) = new Buffer(fm, lm);
  }

  /** Returns the number of available (i.e. unpinned) buffers.
    * @return
    *   the number of available buffers
    */
  def available(): Int = synchronized {
    numAvailable;
  }

  /** Flushes the dirty buffers modified by the specified transaction.
    * @param txnum
    *   the transaction's id number
    */
  def flushAll(txnum: Int) = synchronized {
    bufferpool.foreach(buff => {
      if (buff.modifyingTx() == txnum) {
        buff.flush();
      }
    })
  }

  /** Unpins the specified data buffer. If its pin count goes to zero, then
    * notify any waiting threads.
    * @param buff
    *   the buffer to be unpinned
    */
  def unpin(buff: Buffer) = synchronized {
    buff.unpin();
    if (!buff.isPinned()) {
      numAvailable += 1;
      notifyAll();
    }
  }

  /** Pins a buffer to the specified block, potentially waiting until a buffer
    * becomes available. If no buffer becomes available within a fixed time
    * period, then a {@link BufferAbortException} is thrown.
    * @param blk
    *   a reference to a disk block
    * @return
    *   the buffer pinned to that block
    */
  def pin(blk: BlockId): Buffer = synchronized {
    try {
      val timestamp: Long = System.currentTimeMillis();
      var buff: Buffer = tryToPin(blk);
      while (buff == null && !waitingTooLong(timestamp)) {
        wait(MAX_TIME);
        buff = tryToPin(blk);
      }
      if (buff == null) {
        throw new BufferAbortException();
      }
      buff;
    } catch {
      case e: InterruptedException => throw new BufferAbortException();
    }
  }

  private def waitingTooLong(starttime: Long): Boolean = {
    System.currentTimeMillis() - starttime > MAX_TIME;
  }

  /** Tries to pin a buffer to the specified block. If there is already a buffer
    * assigned to that block then that buffer is used; otherwise, an unpinned
    * buffer from the pool is chosen. Returns a null value if there are no
    * available buffers.
    * @param blk
    *   a reference to a disk block
    * @return
    *   the pinned buffer
    */
  private def tryToPin(blk: BlockId): Buffer = {
    var buff: Buffer = findExistingBuffer(blk);
    if (buff == null) {
      buff = chooseUnpinnedBuffer();
      if (buff == null) {
        return null;
      }
      buff.assignToBlock(blk);
    }
    if (!buff.isPinned()) {
      numAvailable -= 1;
    }
    buff.pin();
    buff;
  }

  private def findExistingBuffer(blk: BlockId): Buffer = {
    bufferpool.foreach(buff => {
      val b: BlockId = buff.block();
      if (b != null && b.equals(blk)) {
        return buff;
      }
    })
    null
  }

  private def chooseUnpinnedBuffer(): Buffer = {
    bufferpool.foreach(buff => {
      if (!buff.isPinned()) {
        return buff;
      }
    })
    null
  }
}
