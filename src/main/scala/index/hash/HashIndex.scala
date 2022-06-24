package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record._;
import simpledb.index.Index;
import simpledb.query._;

/** A static hash implementation of the Index interface. A fixed number of
  * buckets is allocated (currently, 100), and each bucket is implemented as a
  * file of index records.
  * @author
  *   Edward Sciore
  */
/** Opens a hash index for the specified index.
  * @param idxname
  *   the name of the index
  * @param sch
  *   the schema of the index records
  * @param tx
  *   the calling transaction
  */
class HashIndex(val tx: Transaction, val idxname: String, val layout: Layout)
    extends Index {
  val NUM_BUCKETS: Int = 100;

  var searchkey: Constant = null;
  var ts: TableScan = null;

  /** Positions the index before the first index record having the specified
    * search key. The method hashes the search key to determine the bucket, and
    * then opens a table scan on the file corresponding to the bucket. The table
    * scan for the previous bucket (if any) is closed.
    * @see
    *   simpledb.index.Index#beforeFirst(simpledb.query.Constant)
    */
  def beforeFirst(searchkey: Constant): Unit = {
    close();
    this.searchkey = searchkey;
    val bucket: Int = searchkey.hashCode() % NUM_BUCKETS;
    val tblname: String = idxname + bucket;
    ts = new TableScan(tx, tblname, layout);
  }

  /** Moves to the next record having the search key. The method loops through
    * the table scan for the bucket, looking for a matching record, and
    * returning false if there are no more such records.
    * @see
    *   simpledb.index.Index#next()
    */
  def next(): Boolean = {
    while (ts.next()) {
      if (ts.getVal("dataval").equals(searchkey)) {
        true;
      }
    }
    false;
  }

  /** Retrieves the dataRID from the current record in the table scan for the
    * bucket.
    * @see
    *   simpledb.index.Index#getDataRid()
    */
  def getDataRid(): RID = {
    val blknum: Int = ts.getInt("block");
    val id: Int = ts.getInt("id");
    new RID(blknum, id);
  }

  /** Inserts a new record into the table scan for the bucket.
    * @see
    *   simpledb.index.Index#insert(simpledb.query.Constant,
    *   simpledb.record.RID)
    */
  def insert(value: Constant, rid: RID): Unit = {
    beforeFirst(value);
    ts.insert();
    ts.setInt("block", rid.blockNumber());
    ts.setInt("id", rid.getSlot());
    ts.setVal("dataval", value);
  }

  /** Deletes the specified record from the table scan for the bucket. The
    * method starts at the beginning of the scan, and loops through the records
    * until the specified record is found.
    * @see
    *   simpledb.index.Index#delete(simpledb.query.Constant,
    *   simpledb.record.RID)
    */
  def delete(value: Constant, rid: RID): Unit = {
    beforeFirst(value);
    while (next())
      if (getDataRid().equals(rid)) {
        ts.delete();
        return;
      }
  }

  /** Closes the index by closing the current table scan.
    * @see
    *   simpledb.index.Index#close()
    */
  def close(): Unit = {
    if (ts != null)
      ts.close();
  }

  /** Returns the cost of searching an index file having the specified number of
    * blocks. The method assumes that all buckets are about the same size, and
    * so the cost is simply the size of the bucket.
    * @param numblocks
    *   the number of blocks of index records
    * @param rpb
    *   the number of records per block (not used here)
    * @return
    *   the cost of traversing the index
    */
  def searchCost(numblocks: Int, rpb: Int): Int = {
    return numblocks / NUM_BUCKETS;
  }
}
