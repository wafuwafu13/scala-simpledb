package simpledb.record;

/** An identifier for a record within a file. A RID consists of the block number
  * in the file, and the location of the record in that block.
  * @author
  *   Edward Sciore
  */
/** Create a RID for the record having the specified location in the specified
  * block.
  * @param blknum
  *   the block number where the record lives
  * @param slot
  *   the record's loction
  */
class RID(val blknum: Int, val slot: Int) {

  /** Return the block number associated with this RID.
    * @return
    *   the block number
    */
  def blockNumber(): Int = {
    blknum;
  }

  /** Return the slot associated with this RID.
    * @return
    *   the slot
    */
  def getSlot(): Int = {
    slot;
  }

  def ridequals(obj: Object): Boolean = {
    val r: RID = obj.asInstanceOf[RID];
    blknum == r.blknum && slot == r.slot;
  }

  override def toString(): String = {
    "[" + blknum + ", " + slot + "]";
  }
}
