package simpledb.index.btree;

import java.sql.Types.INTEGER;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record._;
import simpledb.index.Index;
import simpledb.query.Constant;

/** A B-tree implementation of the Index interface.
  * @author
  *   Edward Sciore
  */
class BTreeIndex(
    val tx: Transaction,
    val idxname: String,
    val leafLayout: Layout
) extends Index {

  private var leaf: BTreeLeaf = null;

  /** Opens a B-tree index for the specified index. The method determines the
    * appropriate files for the leaf and directory records, creating them if
    * they did not exist.
    * @param idxname
    *   the name of the index
    * @param leafsch
    *   the schema of the leaf index records
    * @param tx
    *   the calling transaction
    */

  // deal with the leaves
  val leaftbl: String = idxname + "leaf";
  if (tx.size(leaftbl) == 0) {
    val blk: BlockId = tx.append(leaftbl);
    val node: BTPage = new BTPage(tx, blk, leafLayout);
    node.format(blk, -1);
  }

  // deal with the directory
  val dirsch: Schema = new Schema();
  dirsch.add("block", leafLayout.getSchema());
  dirsch.add("dataval", leafLayout.getSchema());
  val dirtbl: String = idxname + "dir";
  val dirLayout: Layout = new Layout(dirsch, null, null);
  val rootblk: BlockId = new BlockId(dirtbl, 0);
  if (tx.size(dirtbl) == 0) {
    // create new root block
    tx.append(dirtbl);
    val node: BTPage = new BTPage(tx, rootblk, dirLayout);
    node.format(rootblk, 0);
    // insert initial directory entry
    val fldtype: Int = dirsch.getType("dataval");
    val minval: Constant =
      if (fldtype == INTEGER)
        new Constant(Integer.MIN_VALUE)
      else
        new Constant("");
    node.insertDir(0, minval, 0);
    node.close();
  }

  /** Traverse the directory to find the leaf block corresponding to the
    * specified search key. The method then opens a page for that leaf block,
    * and positions the page before the first record (if any) having that search
    * key. The leaf page is kept open, for use by the methods next and
    * getDataRid.
    * @see
    *   simpledb.index.Index#beforeFirst(simpledb.query.Constant)
    */
  def beforeFirst(searchkey: Constant): Unit = {
    close();
    val root: BTreeDir = new BTreeDir(tx, rootblk, dirLayout);
    val blknum: Int = root.search(searchkey);
    root.close();
    val leafblk: BlockId = new BlockId(leaftbl, blknum);
    leaf = new BTreeLeaf(tx, leafblk, leafLayout, searchkey);
  }

  /** Move to the next leaf record having the previously-specified search key.
    * Returns false if there are no more such leaf records.
    * @see
    *   simpledb.index.Index#next()
    */
  def next(): Boolean = {
    leaf.next();
  }

  /** Return the dataRID value from the current leaf record.
    * @see
    *   simpledb.index.Index#getDataRid()
    */
  def getDataRid(): RID = {
    leaf.getDataRid();
  }

  /** Insert the specified record into the index. The method first traverses the
    * directory to find the appropriate leaf page; then it inserts the record
    * into the leaf. If the insertion causes the leaf to split, then the method
    * calls insert on the root, passing it the directory entry of the new leaf
    * page. If the root node splits, then makeNewRoot is called.
    * @see
    *   simpledb.index.Index#insert(simpledb.query.Constant,
    *   simpledb.record.RID)
    */
  def insert(dataval: Constant, datarid: RID): Unit = {
    beforeFirst(dataval);
    val e: DirEntry = leaf.insert(datarid);
    leaf.close();
    if (e == null)
      return;
    val root: BTreeDir = new BTreeDir(tx, rootblk, dirLayout);
    val e2: DirEntry = root.insert(e);
    if (e2 != null)
      root.makeNewRoot(e2);
    root.close();
  }

  /** Delete the specified index record. The method first traverses the
    * directory to find the leaf page containing that record; then it deletes
    * the record from the page.
    * @see
    *   simpledb.index.Index#delete(simpledb.query.Constant,
    *   simpledb.record.RID)
    */
  def delete(dataval: Constant, datarid: RID): Unit = {
    beforeFirst(dataval);
    leaf.delete(datarid);
    leaf.close();
  }

  /** Close the index by closing its open leaf page, if necessary.
    * @see
    *   simpledb.index.Index#close()
    */
  def close(): Unit = {
    if (leaf != null)
      leaf.close();
  }

  /** Estimate the number of block accesses required to find all index records
    * having a particular search key.
    * @param numblocks
    *   the number of blocks in the B-tree directory
    * @param rpb
    *   the number of index entries per block
    * @return
    *   the estimated traversal cost
    */
  def searchCost(numblocks: Int, rpb: Int): Int = {
    1 + (Math.log(numblocks) / Math.log(rpb)).asInstanceOf[Int];
  }
}
