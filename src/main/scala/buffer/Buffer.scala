package simpledb.buffer;

import simpledb.file._;
import simpledb.log.LogMgr;

/** An individual buffer. A databuffer wraps a page and stores information about
  * its status, such as the associated disk block, the number of times the
  * buffer has been pinned, whether its contents have been modified, and if so,
  * the id and lsn of the modifying transaction.
  * @author
  *   Edward Sciore
  */
class Buffer(val fm: FileMgr, val lm: LogMgr) {

  private val contents: Page = new Page(fm.blockSize());
  private var blk: BlockId = null;
  private var pins: Int = 0;
  private var txnum: Int = -1;
  private var lsn: Int = -1;

  def getContents(): Page = {
    contents;
  }

  /** Returns a reference to the disk block allocated to the buffer.
    * @return
    *   a reference to a disk block
    */
  def block(): BlockId = {
    blk;
  }

  def setModified(txnum: Int, lsn: Int): Unit = {
    this.txnum = txnum;
    if (lsn >= 0)
      this.lsn = lsn;
  }

  /** Return true if the buffer is currently pinned (that is, if it has a
    * nonzero pin count).
    * @return
    *   true if the buffer is pinned
    */
  def isPinned(): Boolean = {
    pins > 0;
  }

  def modifyingTx(): Int = {
    txnum;
  }

  /** Reads the contents of the specified block into the contents of the buffer.
    * If the buffer was dirty, then its previous contents are first written to
    * disk.
    * @param b
    *   a reference to the data block
    */
  def assignToBlock(b: BlockId): Unit = {
    flush();
    blk = b;
    fm.read(blk, contents);
    pins = 0;
  }

  /** Write the buffer to its disk block if it is dirty.
    */
  def flush(): Unit = {
    if (txnum >= 0) {
      lm.flush(lsn);
      fm.write(blk, contents);
      txnum = -1;
    }
  }

  /** Increase the buffer's pin count.
    */
  def pin(): Unit = {
    pins += 1;
  }

  /** Decrease the buffer's pin count.
    */
  def unpin(): Unit = {
    pins -= 1;
  }
}
