package simpledb.index.planner;

import simpledb.record._;
import simpledb.query._;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.index.Index;
import simpledb.index.query.IndexSelectScan;

/** The Plan class corresponding to the <i>indexselect</i> relational algebra
  * operator.
  * @author
  *   Edward Sciore
  */
/** Creates a new indexselect node in the query tree for the specified index and
  * selection constant.
  * @param p
  *   the input table
  * @param ii
  *   information about the index
  * @param val
  *   the selection constant
  */
class IndexSelectPlan(val p: Plan, val ii: IndexInfo, val value: Constant)
    extends Plan {

  /** Creates a new indexselect scan for this query
    * @see
    *   simpledb.plan.Plan#open()
    */
  def open(): Scan = {
    // throws an exception if p is not a tableplan.
    val ts: TableScan = p.open().asInstanceOf[TableScan];
    val idx: Index = ii.open();
    new IndexSelectScan(ts, idx, value);
  }

  /** Estimates the number of block accesses to compute the index selection,
    * which is the same as the index traversal cost plus the number of matching
    * data records.
    * @see
    *   simpledb.plan.Plan#blocksAccessed()
    */
  def blocksAccessed(): Int = {
    ii.blocksAccessed() + recordsOutput();
  }

  /** Estimates the number of output records in the index selection, which is
    * the same as the number of search key values for the index.
    * @see
    *   simpledb.plan.Plan#recordsOutput()
    */
  def recordsOutput(): Int = {
    ii.recordsOutput();
  }

  /** Returns the distinct values as defined by the index.
    * @see
    *   simpledb.plan.Plan#distinctValues(java.lang.String)
    */
  def distinctValues(fldname: String): Int = {
    ii.distinctValues(fldname);
  }

  /** Returns the schema of the data table.
    * @see
    *   simpledb.plan.Plan#schema()
    */
  def schema(): Schema = {
    p.schema();
  }
}
