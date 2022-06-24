package simpledb.index.btree;

import simpledb.query.Constant;

/** A directory entry has two components: the number of the child block, and the
  * dataval of the first record in that block.
  * @author
  *   Edward Sciore
  */
/** Creates a new entry for the specified dataval and block number.
  * @param dataval
  *   the dataval
  * @param blocknum
  *   the block number
  */
class DirEntry(dataval: Constant, blocknum: Int) {

  /** Returns the dataval component of the entry
    * @return
    *   the dataval component of the entry
    */
  def dataVal(): Constant = {
    dataval;
  }

  /** Returns the block number component of the entry
    * @return
    *   the block number component of the entry
    */
  def blockNumber(): Int = {
    blocknum;
  }
}
