package simpledb.index.query;

import simpledb.index.Index;
import simpledb.record.TableScan;
import simpledb.query._;

/** The scan class corresponding to the indexjoin relational algebra operator.
  * The code is very similar to that of ProductScan, which makes sense because
  * an index join is essentially the product of each LHS record with the
  * matching RHS index records.
  * @author
  *   Edward Sciore
  */
/** Creates an index join scan for the specified LHS scan and RHS index.
  * @param lhs
  *   the LHS scan
  * @param idx
  *   the RHS index
  * @param joinfield
  *   the LHS field used for joining
  * @param rhs
  *   the RHS scan
  */
class IndexJoinScan(
    val lhs: Scan,
    val idx: Index,
    val joinfield: String,
    val rhs: TableScan
) extends Scan {

  beforeFirst();

  /** Positions the scan before the first record. That is, the LHS scan will be
    * positioned at its first record, and the index will be positioned before
    * the first record for the join value.
    * @see
    *   simpledb.query.Scan#beforeFirst()
    */
  def beforeFirst(): Unit = {
    lhs.beforeFirst();
    lhs.next();
    resetIndex();
  }

  /** Moves the scan to the next record. The method moves to the next index
    * record, if possible. Otherwise, it moves to the next LHS record and the
    * first index record. If there are no more LHS records, the method returns
    * false.
    * @see
    *   simpledb.query.Scan#next()
    */
  def next(): Boolean = {
    // TODO
    true
    // while (true) {
    //    if (idx.next()) {
    //       rhs.moveToRid(idx.getDataRid());
    //       return true;
    //    }
    //    if (!lhs.next()) {
    //       return false;
    //    }
    //    resetIndex();
    // }
  }

  /** Returns the integer value of the specified field.
    * @see
    *   simpledb.query.Scan#getVal(java.lang.String)
    */
  def getInt(fldname: String): Int = {
    if (rhs.hasField(fldname))
      rhs.getInt(fldname);
    else
      lhs.getInt(fldname);
  }

  /** Returns the Constant value of the specified field.
    * @see
    *   simpledb.query.Scan#getVal(java.lang.String)
    */
  def getVal(fldname: String): Constant = {
    if (rhs.hasField(fldname))
      rhs.getVal(fldname);
    else
      lhs.getVal(fldname);
  }

  /** Returns the string value of the specified field.
    * @see
    *   simpledb.query.Scan#getVal(java.lang.String)
    */
  def getString(fldname: String): String = {
    if (rhs.hasField(fldname))
      rhs.getString(fldname);
    else
      lhs.getString(fldname);
  }

  /** Returns true if the field is in the schema.
    * @see
    *   simpledb.query.Scan#hasField(java.lang.String)
    */
  def hasField(fldname: String): Boolean = {
    rhs.hasField(fldname) || lhs.hasField(fldname);
  }

  /** Closes the scan by closing its LHS scan and its RHS index.
    * @see
    *   simpledb.query.Scan#close()
    */
  def close(): Unit = {
    lhs.close();
    idx.close();
    rhs.close();
  }

  private def resetIndex(): Unit = {
    val searchkey: Constant = lhs.getVal(joinfield);
    idx.beforeFirst(searchkey);
  }
}
