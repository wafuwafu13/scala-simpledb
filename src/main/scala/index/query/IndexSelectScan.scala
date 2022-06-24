package simpledb.index.query;

import simpledb.record.RID;
import simpledb.index.Index;
import simpledb.record.TableScan;
import simpledb.query._;

/** The scan class corresponding to the select relational algebra operator.
  * @author
  *   Edward Sciore
  */
/** Creates an index select scan for the specified index and selection constant.
  * @param idx
  *   the index
  * @param val
  *   the selection constant
  */
class IndexSelectScan(val ts: TableScan, val idx: Index, val value: Constant)
    extends Scan {
  beforeFirst();

  /** Positions the scan before the first record, which in this case means
    * positioning the index before the first instance of the selection constant.
    * @see
    *   simpledb.query.Scan#beforeFirst()
    */
  def beforeFirst(): Unit = {
    idx.beforeFirst(value);
  }

  /** Moves to the next record, which in this case means moving the index to the
    * next record satisfying the selection constant, and returning false if
    * there are no more such index records. If there is a next record, the
    * method moves the tablescan to the corresponding data record.
    * @see
    *   simpledb.query.Scan#next()
    */
  def next(): Boolean = {
    val ok: Boolean = idx.next();
    if (ok) {
      val rid: RID = idx.getDataRid();
      ts.moveToRid(rid);
    }
    ok;
  }

  /** Returns the value of the field of the current data record.
    * @see
    *   simpledb.query.Scan#getInt(java.lang.String)
    */
  def getInt(fldname: String): Int = {
    ts.getInt(fldname);
  }

  /** Returns the value of the field of the current data record.
    * @see
    *   simpledb.query.Scan#getString(java.lang.String)
    */
  def getString(fldname: String): String = {
    ts.getString(fldname);
  }

  /** Returns the value of the field of the current data record.
    * @see
    *   simpledb.query.Scan#getVal(java.lang.String)
    */
  def getVal(fldname: String): Constant = {
    ts.getVal(fldname);
  }

  /** Returns whether the data record has the specified field.
    * @see
    *   simpledb.query.Scan#hasField(java.lang.String)
    */
  def hasField(fldname: String): Boolean = {
    ts.hasField(fldname);
  }

  /** Closes the scan by closing the index and the tablescan.
    * @see
    *   simpledb.query.Scan#close()
    */
  def close(): Unit = {
    idx.close();
    ts.close();
  }
}
