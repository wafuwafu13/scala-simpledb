package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record._;

/** The interface implemented by each query plan. There is a Plan class for each
  * relational algebra operator.
  * @author
  *   Edward Sciore
  */
trait Plan {

  /** Opens a scan corresponding to this plan. The scan will be positioned
    * before its first record.
    * @return
    *   a scan
    */
  def open(): Scan;

  /** Returns an estimate of the number of block accesses that will occur when
    * the scan is read to completion.
    * @return
    *   the estimated number of block accesses
    */
  def blocksAccessed(): Int;

  /** Returns an estimate of the number of records in the query's output table.
    * @return
    *   the estimated number of output records
    */
  def recordsOutput(): Int;

  /** Returns an estimate of the number of distinct values for the specified
    * field in the query's output table.
    * @param fldname
    *   the name of a field
    * @return
    *   the estimated number of distinct field values in the output
    */
  def distinctValues(fldname: String): Int;

  /** Returns the schema of the query.
    * @return
    *   the query's schema
    */
  def schema(): Schema;
}
