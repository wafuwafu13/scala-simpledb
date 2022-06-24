package simpledb.index.btree;

import java.sql.Types.INTEGER;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record._;
import simpledb.query.Constant;

/** B-tree directory and leaf pages have many commonalities: in particular,
  * their records are stored in sorted order, and pages split when full. A
  * BTNode object contains this common functionality.
  * @author
  *   Edward Sciore
  */
class BTPage(val tx: Transaction, var currentblk: BlockId, val layout: Layout) {

  /** Open a node for the specified B-tree block.
    * @param currentblk
    *   a reference to the B-tree block
    * @param layout
    *   the metadata for the particular B-tree file
    * @param tx
    *   the calling transaction
    */
  tx.pin(currentblk);

  /** Calculate the position where the first record having the specified search
    * key should be, then returns the position before it.
    * @param searchkey
    *   the search key
    * @return
    *   the position before where the search key goes
    */
  def findSlotBefore(searchkey: Constant): Int = {
    var slot: Int = 0;
    while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
      slot += 1;
    slot - 1;
  }

  /** Close the page by unpinning its buffer.
    */
  def close(): Unit = {
    if (currentblk != null)
      tx.unpin(currentblk);
    currentblk = null;
  }

  /** Return true if the block is full.
    * @return
    *   true if the block is full
    */
  def isFull(): Boolean = {
    slotpos(getNumRecs() + 1) >= tx.blockSize();
  }

  /** Split the page at the specified position. A new page is created, and the
    * records of the page starting at the split position are transferred to the
    * new page.
    * @param splitpos
    *   the split position
    * @param flag
    *   the initial value of the flag field
    * @return
    *   the reference to the new block
    */
  def split(splitpos: Int, flag: Int): BlockId = {
    val newblk: BlockId = appendNew(flag);
    val newpage: BTPage = new BTPage(tx, newblk, layout);
    transferRecs(splitpos, newpage);
    newpage.setFlag(flag);
    newpage.close();
    newblk;
  }

  /** Return the dataval of the record at the specified slot.
    * @param slot
    *   the integer slot of an index record
    * @return
    *   the dataval of the record at that slot
    */
  def getDataVal(slot: Int): Constant = {
    getVal(slot, "dataval");
  }

  /** Return the value of the page's flag field
    * @return
    *   the value of the page's flag field
    */
  def getFlag(): Int = {
    tx.getInt(currentblk, 0);
  }

  /** Set the page's flag field to the specified value
    * @param val
    *   the new value of the page flag
    */
  def setFlag(value: Int): Unit = {
    tx.setInt(currentblk, 0, value, true);
  }

  /** Append a new block to the end of the specified B-tree file, having the
    * specified flag value.
    * @param flag
    *   the initial value of the flag
    * @return
    *   a reference to the newly-created block
    */
  def appendNew(flag: Int): BlockId = {
    val blk: BlockId = tx.append(currentblk.fileName());
    tx.pin(blk);
    format(blk, flag);
    blk;
  }

  def format(blk: BlockId, flag: Int): Unit = {
    tx.setInt(blk, 0, flag, false);
    tx.setInt(blk, Integer.BYTES, 0, false); // #records = 0
    val recsize: Int = layout.slotSize();
    // for (val pos: Int=2*Integer.BYTES; pos+recsize<=tx.blockSize(); pos += recsize)
    //    makeDefaultRecord(blk, pos);
  }

  private def makeDefaultRecord(blk: BlockId, pos: Int): Unit = {
    layout
      .getSchema()
      .getFields()
      .forEach(fldname => {
        val offset = layout.offset(fldname);
        if (layout.getSchema().getType(fldname) == INTEGER)
          tx.setInt(blk, pos + offset, 0, false);
        else
          tx.setString(blk, pos + offset, "", false);

      })

  }
  // Methods called only by BTreeDir

  /** Return the block number stored in the index record at the specified slot.
    * @param slot
    *   the slot of an index record
    * @return
    *   the block number stored in that record
    */
  def getChildNum(slot: Int): Int = {
    getInt(slot, "block");
  }

  /** Insert a directory entry at the specified slot.
    * @param slot
    *   the slot of an index record
    * @param val
    *   the dataval to be stored
    * @param blknum
    *   the block number to be stored
    */
  def insertDir(slot: Int, value: Constant, blknum: Int): Unit = {
    insert(slot);
    setVal(slot, "dataval", value);
    setInt(slot, "block", blknum);
  }

  // Methods called only by BTreeLeaf

  /** Return the dataRID value stored in the specified leaf index record.
    * @param slot
    *   the slot of the desired index record
    * @return
    *   the dataRID value store at that slot
    */
  def getDataRid(slot: Int): RID = {
    new RID(getInt(slot, "block"), getInt(slot, "id"));
  }

  /** Insert a leaf index record at the specified slot.
    * @param slot
    *   the slot of the desired index record
    * @param val
    *   the new dataval
    * @param rid
    *   the new dataRID
    */
  def insertLeaf(slot: Int, value: Constant, rid: RID): Unit = {
    insert(slot);
    setVal(slot, "dataval", value);
    setInt(slot, "block", rid.blockNumber());
    setInt(slot, "id", rid.getSlot());
  }

  /** Delete the index record at the specified slot.
    * @param slot
    *   the slot of the deleted index record
    */
  def delete(slot: Int): Unit = {
    // for (int i=slot+1; i<getNumRecs(); i++)
    //    copyRecord(i, i-1);
    setNumRecs(getNumRecs() - 1);
  }

  /** Return the number of index records in this page.
    * @return
    *   the number of index records in this page
    */
  def getNumRecs(): Int = {
    tx.getInt(currentblk, Integer.BYTES);
  }

  // Private methods

  private def getInt(slot: Int, fldname: String): Int = {
    val pos: Int = fldpos(slot, fldname);
    return tx.getInt(currentblk, pos);
  }

  private def getString(slot: Int, fldname: String): String = {
    val pos: Int = fldpos(slot, fldname);
    return tx.getString(currentblk, pos);
  }

  private def getVal(slot: Int, fldname: String): Constant = {
    val typeval: Int = layout.getSchema().getType(fldname);
    if (typeval == INTEGER)
      return new Constant(getInt(slot, fldname));
    else
      return new Constant(getString(slot, fldname));
  }

  private def setInt(slot: Int, fldname: String, value: Int): Unit = {
    val pos: Int = fldpos(slot, fldname);
    tx.setInt(currentblk, pos, value, true);
  }

  private def setString(slot: Int, fldname: String, value: String): Unit = {
    val pos: Int = fldpos(slot, fldname);
    tx.setString(currentblk, pos, value, true);
  }

  private def setVal(slot: Int, fldname: String, value: Constant): Unit = {
    val typeval: Int = layout.getSchema().getType(fldname);
    if (typeval == INTEGER)
      setInt(slot, fldname, value.asInt());
    else
      setString(slot, fldname, value.asString());
  }

  private def setNumRecs(n: Int): Unit = {
    tx.setInt(currentblk, Integer.BYTES, n, true);
  }

  private def insert(slot: Int): Unit = {
    // for (int i=getNumRecs(); i>slot; i--)
    //    copyRecord(i-1, i);
    setNumRecs(getNumRecs() + 1);
  }

  private def copyRecord(from: Int, to: Int): Unit = {
    val sch: Schema = layout.getSchema();
    sch
      .getFields()
      .forEach(fldname => {
        setVal(to, fldname, getVal(from, fldname));
      })
  }

  private def transferRecs(slot: Int, dest: BTPage): Unit = {
    var destslot: Int = 0;
    while (slot < getNumRecs()) {
      dest.insert(destslot);
      val sch: Schema = layout.getSchema();
      sch
        .getFields()
        .forEach(fldname => {
          dest.setVal(destslot, fldname, getVal(slot, fldname));
        })
      delete(slot);
      destslot += 1;
    }
  }

  private def fldpos(slot: Int, fldname: String): Int = {
    val offset: Int = layout.offset(fldname);
    return slotpos(slot) + offset;
  }

  private def slotpos(slot: Int): Int = {
    val slotsize: Int = layout.slotSize();
    return Integer.BYTES + Integer.BYTES + (slot * slotsize);
  }
}
