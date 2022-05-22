package simpledb.plan;

import java.util.List;
import simpledb.record.Schema;
import simpledb.query._;

/** The Plan class corresponding to the <i>project</i> relational algebra
  * operator.
  * @author
  *   Edward Sciore
  */
/** Creates a new project node in the query tree, having the specified subquery
  * and field list.
  * @param p
  *   the subquery
  * @param fieldlist
  *   the list of fields
  */
class ProjectPlan(val p: Plan, val fieldlist: List[String]) extends Plan {
  private val _schema: Schema = new Schema();
  fieldlist.forEach(fldname => {
    _schema.add(fldname, p.schema());
  })

  /** Creates a project scan for this query.
    * @see
    *   simpledb.plan.Plan#open()
    */
  def open(): Scan = {
    val s: Scan = p.open();
    new ProjectScan(s, _schema.getFields());
  }

  /** Estimates the number of block accesses in the projection, which is the
    * same as in the underlying query.
    * @see
    *   simpledb.plan.Plan#blocksAccessed()
    */
  def blocksAccessed(): Int = {
    p.blocksAccessed();
  }

  /** Estimates the number of output records in the projection, which is the
    * same as in the underlying query.
    * @see
    *   simpledb.plan.Plan#recordsOutput()
    */
  def recordsOutput(): Int = {
    p.recordsOutput();
  }

  /** Estimates the number of distinct field values in the projection, which is
    * the same as in the underlying query.
    * @see
    *   simpledb.plan.Plan#distinctValues(java.lang.String)
    */
  def distinctValues(fldname: String): Int = {
    p.distinctValues(fldname);
  }

  /** Returns the schema of the projection, which is taken from the field list.
    * @see
    *   simpledb.plan.Plan#schema()
    */
  def schema(): Schema = {
    _schema;
  }
}
