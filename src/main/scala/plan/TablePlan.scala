package simpledb.plan;

import simpledb.tx.Transaction;
import simpledb.metadata._;
import simpledb.query.Scan;
import simpledb.record._;

/** The Plan class corresponding to a table.
  * @author
  *   Edward Sciore
  */
/** Creates a leaf node in the query tree corresponding to the specified table.
  * @param tblname
  *   the name of the table
  * @param tx
  *   the calling transaction
  */
class TablePlan(val tx: Transaction, val tblname: String, val md: MetadataMgr)
    extends Plan {

  private val layout: Layout = md.getLayout(tblname, tx);
  private val si: StatInfo = md.getStatInfo(tblname, layout, tx);;

  /** Creates a table scan for this query.
    * @see
    *   simpledb.plan.Plan#open()
    */
  def open(): Scan = {
    new TableScan(tx, tblname, layout);
  }

  /** Estimates the number of block accesses for the table, which is obtainable
    * from the statistics manager.
    * @see
    *   simpledb.plan.Plan#blocksAccessed()
    */
  def blocksAccessed(): Int = {
    si.blocksAccessed();
  }

  /** Estimates the number of records in the table, which is obtainable from the
    * statistics manager.
    * @see
    *   simpledb.plan.Plan#recordsOutput()
    */
  def recordsOutput(): Int = {
    si.recordsOutput();
  }

  /** Estimates the number of distinct field values in the table, which is
    * obtainable from the statistics manager.
    * @see
    *   simpledb.plan.Plan#distinctValues(java.lang.String)
    */
  def distinctValues(fldname: String): Int = {
    si.distinctValues(fldname);
  }

  /** Determines the schema of the table, which is obtainable from the catalog
    * manager.
    * @see
    *   simpledb.plan.Plan#schema()
    */
  def schema(): Schema = {
    layout.getSchema();
  }
}
