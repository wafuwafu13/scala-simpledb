package simpledb.tx;

import java.util._;
import simpledb.file.BlockId;
import simpledb.buffer._;
import collection.JavaConverters._;

/** Manage the transaction's currently-pinned buffers.
  * @author
  *   Edward Sciore
  */
class BufferList(val bm: BufferMgr) {
  val pins: java.util.List[BlockId] = new java.util.ArrayList[BlockId]()
  private val buffers: Map[BlockId, Buffer] =
    scala.collection.mutable.HashMap().asJava;

  /** Return the buffer pinned to the specified block. The method returns null
    * if the transaction has not pinned the block.
    * @param blk
    *   a reference to the disk block
    * @return
    *   the buffer pinned to that block
    */
  def getBuffer(blk: BlockId): Buffer = {
    buffers.get(blk);
  }

  /** Pin the block and keep track of the buffer internally.
    * @param blk
    *   a reference to the disk block
    */
  def pin(blk: BlockId): Unit = {
    val buff: Buffer = bm.pin(blk);
    buffers.put(blk, buff);
    pins.add(blk);
  }

  /** Unpin the specified block.
    * @param blk
    *   a reference to the disk block
    */
  def unpin(blk: BlockId): Unit = {
    val buff: Buffer = buffers.get(blk);
    bm.unpin(buff);
    pins.remove(blk);
    if (!pins.contains(blk)) {
      buffers.remove(blk);
    }
  }

  /** Unpin any buffers still pinned by this transaction.
    */
  def unpinAll(): Unit = {
    pins.forEach(blk => {
      val buff: Buffer = buffers.get(blk);
      bm.unpin(buff);
    })
    buffers.clear();
    pins.clear();
  }
}
