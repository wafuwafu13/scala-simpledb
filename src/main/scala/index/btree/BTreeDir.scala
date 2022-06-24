package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.tx.Transaction;
import simpledb.record.Layout;

/** A B-tree directory block.
  * @author
  *   Edward Sciore
  */
class BTreeDir(val tx: Transaction, val blk: BlockId, val layout: Layout) {

  /** Creates an object to hold the contents of the specified B-tree block.
    * @param blk
    *   a reference to the specified B-tree block
    * @param layout
    *   the metadata of the B-tree directory file
    * @param tx
    *   the calling transaction
    */
  var contents: BTPage = new BTPage(tx, blk, layout);
  val filename: String = blk.fileName();

  /** Closes the directory page.
    */
  def close(): Unit = {
    contents.close();
  }

  /** Returns the block number of the B-tree leaf block that contains the
    * specified search key.
    * @param searchkey
    *   the search key value
    * @return
    *   the block number of the leaf block containing that search key
    */
  def search(searchkey: Constant): Int = {
    var childblk: BlockId = findChildBlock(searchkey);
    while (contents.getFlag() > 0) {
      contents.close();
      contents = new BTPage(tx, childblk, layout);
      childblk = findChildBlock(searchkey);
    }
    childblk.number();
  }

  /** Creates a new root block for the B-tree. The new root will have two
    * children: the old root, and the specified block. Since the root must
    * always be in block 0 of the file, the contents of the old root will get
    * transferred to a new block.
    * @param e
    *   the directory entry to be added as a child of the new root
    */
  def makeNewRoot(e: DirEntry): Unit = {
    val firstval: Constant = contents.getDataVal(0);
    val level: Int = contents.getFlag();
    val newblk: BlockId =
      contents.split(0, level); // ie, transfer all the records
    val oldroot: DirEntry = new DirEntry(firstval, newblk.number());
    insertEntry(oldroot);
    insertEntry(e);
    contents.setFlag(level + 1);
  }

  /** Inserts a new directory entry into the B-tree block. If the block is at
    * level 0, then the entry is inserted there. Otherwise, the entry is
    * inserted into the appropriate child node, and the return value is
    * examined. A non-null return value indicates that the child node split, and
    * so the returned entry is inserted into this block. If this block splits,
    * then the method similarly returns the entry information of the new block
    * to its caller; otherwise, the method returns null.
    * @param e
    *   the directory entry to be inserted
    * @return
    *   the directory entry of the newly-split block, if one exists; otherwise,
    *   null
    */
  def insert(e: DirEntry): DirEntry = {
    if (contents.getFlag() == 0)
      return insertEntry(e);
    val childblk: BlockId = findChildBlock(e.dataVal());
    val child: BTreeDir = new BTreeDir(tx, childblk, layout);
    val myentry: DirEntry = child.insert(e);
    child.close();
    if (myentry != null) insertEntry(myentry) else null;
  }

  private def insertEntry(e: DirEntry): DirEntry = {
    val newslot: Int = 1 + contents.findSlotBefore(e.dataVal());
    contents.insertDir(newslot, e.dataVal(), e.blockNumber());
    if (!contents.isFull())
      return null;
    // else page is full, so split it
    val level: Int = contents.getFlag();
    val splitpos: Int = contents.getNumRecs() / 2;
    val splitval: Constant = contents.getDataVal(splitpos);
    val newblk: BlockId = contents.split(splitpos, level);
    new DirEntry(splitval, newblk.number());
  }

  private def findChildBlock(searchkey: Constant): BlockId = {
    var slot: Int = contents.findSlotBefore(searchkey);
    if (contents.getDataVal(slot + 1).equals(searchkey))
      slot += 1;
    val blknum: Int = contents.getChildNum(slot);
    new BlockId(filename, blknum);
  }
}
