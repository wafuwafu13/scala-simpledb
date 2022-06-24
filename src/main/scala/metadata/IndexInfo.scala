package simpledb.metadata;

import java.sql.Types.INTEGER;
import simpledb.tx.Transaction;
import simpledb.record._;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
// import simpledb.index.btree.BTreeIndex; //in case we change to btree indexing

/** The information about an index. This information is used by the query
  * planner in order to estimate the costs of using the index, and to obtain the
  * layout of the index records. Its methods are essentially the same as those
  * of Plan.
  * @author
  *   Edward Sciore
  */
class IndexInfo(
    val idxname: String,
    val fldname: String,
    val tblSchema: Schema,
    val tx: Transaction,
    val si: StatInfo
) {

  /** Create an IndexInfo object for the specified index.
    * @param idxname
    *   the name of the index
    * @param fldname
    *   the name of the indexed field
    * @param tx
    *   the calling transaction
    * @param tblSchema
    *   the schema of the table
    * @param si
    *   the statistics for the table
    */

  private val idxLayout: Layout = createIdxLayout();

  /** Open the index described by this object.
    * @return
    *   the Index object associated with this information
    */
  def open(): Index = {
    return new HashIndex(tx, idxname, idxLayout);
    // return new BTreeIndex(tx, idxname, idxLayout);
  }

  /** Estimate the number of block accesses required to find all index records
    * having a particular search key. The method uses the table's metadata to
    * estimate the size of the index file and the number of index records per
    * block. It then passes this information to the traversalCost method of the
    * appropriate index type, which provides the estimate.
    * @return
    *   the number of block accesses required to traverse the index
    */
  def blocksAccessed(): Int = {
    val rpb: Int = tx.blockSize() / idxLayout.slotSize();
    val numblocks: Int = si.recordsOutput() / rpb;
    // HashIndex.searchCost(numblocks, rpb);
    numblocks / 100
    // BTreeIndex.searchCost(numblocks, rpb);
  }

  /** Return the estimated number of records having a search key. This value is
    * the same as doing a select query; that is, it is the number of records in
    * the table divided by the number of distinct values of the indexed field.
    * @return
    *   the estimated number of records having a search key
    */
  def recordsOutput(): Int = {
    si.recordsOutput() / si.distinctValues(fldname);
  }

  /** Return the distinct values for a specified field in the underlying table,
    * or 1 for the indexed field.
    * @param fname
    *   the specified field
    */
  def distinctValues(fname: String): Int = {
    if (fldname.equals(fname)) 1 else si.distinctValues(fldname);
  }

  /** Return the layout of the index records. The schema consists of the dataRID
    * (which is represented as two integers, the block number and the record ID)
    * and the dataval (which is the indexed field). Schema information about the
    * indexed field is obtained via the table's schema.
    * @return
    *   the layout of the index records
    */
  private def createIdxLayout(): Layout = {
    val sch: Schema = new Schema();
    sch.addIntField("block");
    sch.addIntField("id");
    if (tblSchema.getType(fldname) == INTEGER)
      sch.addIntField("dataval");
    else {
      val fldlen: Int = tblSchema.length(fldname);
      sch.addStringField("dataval", fldlen);
    }
    new Layout(sch, null, null);
  }
}
