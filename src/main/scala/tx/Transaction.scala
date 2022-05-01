package simpledb.tx;

import simpledb.file._;
import simpledb.log.LogMgr;
import simpledb.buffer._;
import simpledb.tx.recovery._;
import simpledb.tx.concurrency.ConcurrencyMgr;

/** Provide transaction management for clients, ensuring that all transactions
  * are serializable, recoverable, and in general satisfy the ACID properties.
  * @author
  *   Edward Sciore
  */
class Transaction(val fm: FileMgr, val lm: LogMgr, val bm: BufferMgr) {

  /** Create a new transaction and its associated recovery and concurrency
    * managers. This constructor depends on the file, log, and buffer managers
    * that it gets from the class {@link simpledb.server.SimpleDB}. Those
    * objects are created during system initialization. Thus this constructor
    * cannot be called until either {@link
    * simpledb.server.SimpleDB#init(String)} or {@link
    * simpledb.server.SimpleDB#initFileLogAndBufferMgr(String)} or is called
    * first.
    */
  private var nextTxNum: Int = 0;
  private final val END_OF_FILE: Int = -1;
  private val txnum: Int = nextTxNumber();
  private val recoveryMgr: RecoveryMgr = new RecoveryMgr(this, txnum, lm, bm);
  private val concurMgr: ConcurrencyMgr = new ConcurrencyMgr();
  private val mybuffers: BufferList = new BufferList(bm);

  /** Commit the current transaction. Flush all modified buffers (and their log
    * records), write and flush a commit record to the log, release all locks,
    * and unpin any pinned buffers.
    */
  def commit(): Unit = {
    recoveryMgr.commit();
    println("transaction " + txnum + " committed");
    concurMgr.release();
    mybuffers.unpinAll();
  }

  /** Rollback the current transaction. Undo any modified values, flush those
    * buffers, write and flush a rollback record to the log, release all locks,
    * and unpin any pinned buffers.
    */
  def rollback(): Unit = {
    recoveryMgr.rollback();
    println("transaction " + txnum + " rolled back");
    concurMgr.release();
    mybuffers.unpinAll();
  }

  /** Flush all modified buffers. Then go through the log, rolling back all
    * uncommitted transactions. Finally, write a quiescent checkpoint record to
    * the log. This method is called during system startup, before user
    * transactions begin.
    */
  def recover(): Unit = {
    bm.flushAll(txnum);
    recoveryMgr.recover();
  }

  /** Pin the specified block. The transaction manages the buffer for the
    * client.
    * @param blk
    *   a reference to the disk block
    */
  def pin(blk: BlockId): Unit = {
    mybuffers.pin(blk);
  }

  /** Unpin the specified block. The transaction looks up the buffer pinned to
    * this block, and unpins it.
    * @param blk
    *   a reference to the disk block
    */
  def unpin(blk: BlockId): Unit = {
    mybuffers.unpin(blk);
  }

  /** Return the integer value stored at the specified offset of the specified
    * block. The method first obtains an SLock on the block, then it calls the
    * buffer to retrieve the value.
    * @param blk
    *   a reference to a disk block
    * @param offset
    *   the byte offset within the block
    * @return
    *   the integer stored at that offset
    */
  def getInt(blk: BlockId, offset: Int): Int = {
    concurMgr.sLock(blk);
    val buff: Buffer = mybuffers.getBuffer(blk);
    buff.getContents().getInt(offset);
  }

  /** Return the string value stored at the specified offset of the specified
    * block. The method first obtains an SLock on the block, then it calls the
    * buffer to retrieve the value.
    * @param blk
    *   a reference to a disk block
    * @param offset
    *   the byte offset within the block
    * @return
    *   the string stored at that offset
    */
  def getString(blk: BlockId, offset: Int): String = {
    concurMgr.sLock(blk);
    val buff: Buffer = mybuffers.getBuffer(blk);
    buff.getContents().getString(offset);
  }

  /** Store an integer at the specified offset of the specified block. The
    * method first obtains an XLock on the block. It then reads the current
    * value at that offset, puts it into an update log record, and writes that
    * record to the log. Finally, it calls the buffer to store the value,
    * passing in the LSN of the log record and the transaction's id.
    * @param blk
    *   a reference to the disk block
    * @param offset
    *   a byte offset within that block
    * @param val
    *   the value to be stored
    */
  def setInt(blk: BlockId, offset: Int, value: Int, okToLog: Boolean): Unit = {
    concurMgr.xLock(blk);
    val buff: Buffer = mybuffers.getBuffer(blk);
    var lsn: Int = -1;
    if (okToLog) {
      lsn = recoveryMgr.setInt(buff, offset, value);
    }
    val p: Page = buff.getContents();
    p.setInt(offset, value);
    buff.setModified(txnum, lsn);
  }

  /** Store a string at the specified offset of the specified block. The method
    * first obtains an XLock on the block. It then reads the current value at
    * that offset, puts it into an update log record, and writes that record to
    * the log. Finally, it calls the buffer to store the value, passing in the
    * LSN of the log record and the transaction's id.
    * @param blk
    *   a reference to the disk block
    * @param offset
    *   a byte offset within that block
    * @param val
    *   the value to be stored
    */
  def setString(
      blk: BlockId,
      offset: Int,
      value: String,
      okToLog: Boolean
  ): Unit = {
    concurMgr.xLock(blk);
    val buff: Buffer = mybuffers.getBuffer(blk);
    var lsn: Int = -1;
    if (okToLog) {
      lsn = recoveryMgr.setString(buff, offset, value);
    }
    val p: Page = buff.getContents();
    p.setString(offset, value);
    buff.setModified(txnum, lsn);
  }

  /** Return the number of blocks in the specified file. This method first
    * obtains an SLock on the "end of the file", before asking the file manager
    * to return the file size.
    * @param filename
    *   the name of the file
    * @return
    *   the number of blocks in the file
    */
  def size(filename: String): Int = {
    val dummyblk: BlockId = new BlockId(filename, END_OF_FILE);
    concurMgr.sLock(dummyblk);
    fm.length(filename);
  }

  /** Append a new block to the end of the specified file and returns a
    * reference to it. This method first obtains an XLock on the "end of the
    * file", before performing the append.
    * @param filename
    *   the name of the file
    * @return
    *   a reference to the newly-created disk block
    */
  def append(filename: String): BlockId = {
    val dummyblk: BlockId = new BlockId(filename, END_OF_FILE);
    concurMgr.xLock(dummyblk);
    fm.append(filename);
  }

  def blockSize(): Int = {
    fm.blockSize();
  }

  def availableBuffs(): Int = {
    bm.available();
  }

  private def nextTxNumber(): Int = synchronized {
    nextTxNum += 1;
    nextTxNum;
  }
}
