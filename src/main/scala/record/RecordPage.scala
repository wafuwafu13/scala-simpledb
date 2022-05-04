package simpledb.record;

import java.sql.Types.INTEGER;
import simpledb.file._;
import simpledb.tx.Transaction;

/** Store a record at a given location in a block.
  * @author
  *   Edward Sciore
  */
class RecordPage(val tx: Transaction, val blk: BlockId, val layout: Layout) {
  val EMPTY: Int = 0
  val USED: Int = 1;
  tx.pin(blk);

  /** Return the integer value stored for the specified field of a specified
    * slot.
    * @param fldname
    *   the name of the field.
    * @return
    *   the integer stored in that field
    */
  def getInt(slot: Int, fldname: String): Int = {
    val fldpos: Int = offset(slot) + layout.offset(fldname);
    tx.getInt(blk, fldpos);
  }

  /** Return the string value stored for the specified field of the specified
    * slot.
    * @param fldname
    *   the name of the field.
    * @return
    *   the string stored in that field
    */
  def getString(slot: Int, fldname: String): String = {
    val fldpos: Int = offset(slot) + layout.offset(fldname);
    tx.getString(blk, fldpos);
  }

  /** Store an integer at the specified field of the specified slot.
    * @param fldname
    *   the name of the field
    * @param val
    *   the integer value stored in that field
    */
  def setInt(slot: Int, fldname: String, value: Int): Unit = {
    val fldpos: Int = offset(slot) + layout.offset(fldname);
    tx.setInt(blk, fldpos, value, true);
  }

  /** Store a string at the specified field of the specified slot.
    * @param fldname
    *   the name of the field
    * @param val
    *   the string value stored in that field
    */
  def setString(slot: Int, fldname: String, value: String): Unit = {
    val fldpos: Int = offset(slot) + layout.offset(fldname);
    tx.setString(blk, fldpos, value, true);
  }

  def delete(slot: Int): Unit = {
    setFlag(slot, EMPTY);
  }

  /** Use the layout to format a new block of records. These values should not
    * be logged (because the old values are meaningless).
    */
  def format(): Unit = {
    var slot: Int = 0;
    while (isValidSlot(slot)) {
      tx.setInt(blk, offset(slot), EMPTY, false);
      val sch: Schema = layout.getSchema();
      sch
        .getFields()
        .forEach(fldname => {
          val fldpos: Int = offset(slot) + layout.offset(fldname);
          if (sch.getType(fldname) == INTEGER)
            tx.setInt(blk, fldpos, 0, false);
          else
            tx.setString(blk, fldpos, "", false);
        })
      slot += 1;
    }
  }

  def nextAfter(slot: Int): Int = {
    searchAfter(slot, USED);
  }

  def insertAfter(slot: Int): Int = {
    val newslot: Int = searchAfter(slot, EMPTY);
    if (newslot >= 0) { setFlag(newslot, USED); }

    newslot;
  }

  def block(): BlockId = {
    blk;
  }

  // Private auxiliary methods

  /** Set the record's empty/inuse flag.
    */
  private def setFlag(slot: Int, flag: Int): Unit = {
    tx.setInt(blk, offset(slot), flag, true);
  }

  private def searchAfter(slot: Int, flag: Int): Int = {
    var resultslot = slot;
    resultslot += 1;
    while (isValidSlot(resultslot)) {
      if (tx.getInt(blk, offset(resultslot)) == flag) {
        return resultslot;
      }
      resultslot += 1;
    }
    -1;
  }

  private def isValidSlot(slot: Int): Boolean = {
    offset(slot + 1) <= tx.blockSize();
  }

  private def offset(slot: Int): Int = {
    slot * layout.slotSize();
  }
}
