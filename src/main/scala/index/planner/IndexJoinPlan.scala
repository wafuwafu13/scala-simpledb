package simpledb.index.planner;

import simpledb.record._;
import simpledb.query._;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.index.Index;
import simpledb.index.query.IndexJoinScan;

/** The Plan class corresponding to the <i>indexjoin</i> relational algebra
  * operator.
  * @author
  *   Edward Sciore
  */
class IndexJoinPlan(
    val p1: Plan,
    val p2: Plan,
    val ii: IndexInfo,
    val joinfield: String
) extends Plan {
  val sch: Schema = new Schema();

  /** Implements the join operator, using the specified LHS and RHS plans.
    * @param p1
    *   the left-hand plan
    * @param p2
    *   the right-hand plan
    * @param ii
    *   information about the right-hand index
    * @param joinfield
    *   the left-hand field used for joining
    */

  sch.addAll(p1.schema());
  sch.addAll(p2.schema());

  /** Opens an indexjoin scan for this query
    * @see
    *   simpledb.plan.Plan#open()
    */
  def open(): Scan = {
    val s: Scan = p1.open();
    // throws an exception if p2 is not a tableplan
    val ts: TableScan = p2.open().asInstanceOf[TableScan];
    val idx: Index = ii.open();
    new IndexJoinScan(s, idx, joinfield, ts);
  }

  /** Estimates the number of block accesses to compute the join. The formula
    * is: <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx) +
    * R(indexjoin(p1,p2,idx) </pre>
    * @see
    *   simpledb.plan.Plan#blocksAccessed()
    */
  def blocksAccessed(): Int = {
    p1.blocksAccessed()
    +(p1.recordsOutput() * ii.blocksAccessed())
    +recordsOutput();
  }

  /** Estimates the number of output records in the join. The formula is: <pre>
    * R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    * @see
    *   simpledb.plan.Plan#recordsOutput()
    */
  def recordsOutput(): Int = {
    p1.recordsOutput() * ii.recordsOutput();
  }

  /** Estimates the number of distinct values for the specified field.
    * @see
    *   simpledb.plan.Plan#distinctValues(java.lang.String)
    */
  def distinctValues(fldname: String): Int = {
    if (p1.schema().hasField(fldname))
      p1.distinctValues(fldname);
    else
      p2.distinctValues(fldname);
  }

  /** Returns the schema of the index join.
    * @see
    *   simpledb.plan.Plan#schema()
    */
  def schema(): Schema = {
    sch;
  }
}
