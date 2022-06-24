package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record._;
import simpledb.query.Constant;

/** An object that holds the contents of a B-tree leaf block.
  * @author
  *   Edward Sciore
  */
class BTreeLeaf(
    tx: Transaction,
    blk: BlockId,
    layout: Layout,
    searchkey: Constant
) {

  /** Opens a buffer to hold the specified leaf block. The buffer is positioned
    * immediately before the first record having the specified search key (if
    * any).
    * @param blk
    *   a reference to the disk block
    * @param layout
    *   the metadata of the B-tree leaf file
    * @param searchkey
    *   the search key value
    * @param tx
    *   the calling transaction
    */
  var contents: BTPage = new BTPage(tx, blk, layout);
  var currentslot: Int = contents.findSlotBefore(searchkey);
  val filename: String = blk.fileName();

  /** Closes the leaf page.
    */
  def close(): Unit = {
    contents.close();
  }

  /** Moves to the next leaf record having the previously-specified search key.
    * Returns false if there is no more such records.
    * @return
    *   false if there are no more leaf records for the search key
    */
  def next(): Boolean = {
    currentslot += 1;
    if (currentslot >= contents.getNumRecs())
      return tryOverflow();
    else if (contents.getDataVal(currentslot).equals(searchkey))
      return true;
    else
      return tryOverflow();
  }

  /** Returns the dataRID value of the current leaf record.
    * @return
    *   the dataRID of the current record
    */
  def getDataRid(): RID = {
    return contents.getDataRid(currentslot);
  }

  /** Deletes the leaf record having the specified dataRID
    * @param datarid
    *   the dataRId whose record is to be deleted
    */
  def delete(datarid: RID): Unit = {
    while (next())
      if (getDataRid().equals(datarid)) {
        contents.delete(currentslot);
        return;
      }
  }

  /** Inserts a new leaf record having the specified dataRID and the
    * previously-specified search key. If the record does not fit in the page,
    * then the page splits and the method returns the directory entry for the
    * new page; otherwise, the method returns null. If all of the records in the
    * page have the same dataval, then the block does not split; instead, all
    * but one of the records are placed into an overflow block.
    * @param datarid
    *   the dataRID value of the new record
    * @return
    *   the directory entry of the newly-split page, if one exists.
    */
  def insert(datarid: RID): DirEntry = {
    if (
      contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0
    ) {
      val firstval: Constant = contents.getDataVal(0);
      val newblk: BlockId = contents.split(0, contents.getFlag());
      currentslot = 0;
      contents.setFlag(-1);
      contents.insertLeaf(currentslot, searchkey, datarid);
      new DirEntry(firstval, newblk.number());
    }

    currentslot += 1;
    contents.insertLeaf(currentslot, searchkey, datarid);
    if (!contents.isFull())
      return null;
    // else page is full, so split it
    val firstkey: Constant = contents.getDataVal(0);
    val lastkey: Constant = contents.getDataVal(contents.getNumRecs() - 1);
    if (lastkey.equals(firstkey)) {
      // create an overflow block to hold all but the first record
      val newblk: BlockId = contents.split(1, contents.getFlag());
      contents.setFlag(newblk.number());
      return null;
    } else {
      var splitpos: Int = contents.getNumRecs() / 2;
      var splitkey: Constant = contents.getDataVal(splitpos);
      if (splitkey.equals(firstkey)) {
        // move right, looking for the next key
        while (contents.getDataVal(splitpos).equals(splitkey))
          splitpos += 1;
        splitkey = contents.getDataVal(splitpos);
      } else {
        // move left, looking for first entry having that key
        while (contents.getDataVal(splitpos - 1).equals(splitkey))
          splitpos -= 1;
      }
      val newblk: BlockId = contents.split(splitpos, -1);
      return new DirEntry(splitkey, newblk.number());
    }
  }

  private def tryOverflow(): Boolean = {
    val firstkey: Constant = contents.getDataVal(0);
    val flag: Int = contents.getFlag();
    if (!searchkey.equals(firstkey) || flag < 0)
      return false;
    contents.close();
    val nextblk: BlockId = new BlockId(filename, flag);
    contents = new BTPage(tx, nextblk, layout);
    currentslot = 0;
    return true;
  }
}
