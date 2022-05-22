package simpledb.plan;

import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

/** The Plan class corresponding to the <i>product</i> relational algebra
  * operator.
  * @author
  *   Edward Sciore
  */
/** Creates a new product node in the query tree, having the two specified
  * subqueries.
  * @param p1
  *   the left-hand subquery
  * @param p2
  *   the right-hand subquery
  */
class ProductPlan(val p1: Plan, val p2: Plan) extends Plan {
  private val _schema: Schema = new Schema();

  _schema.addAll(p1.schema());
  _schema.addAll(p2.schema());

  /** Creates a product scan for this query.
    * @see
    *   simpledb.plan.Plan#open()
    */
  def open(): Scan = {
    val s1: Scan = p1.open();
    val s2: Scan = p2.open();
    new ProductScan(s1, s2);
  }

  /** Estimates the number of block accesses in the product. The formula is:
    * <pre> B(product(p1,p2)) = B(p1) + R(p1)*B(p2) </pre>
    * @see
    *   simpledb.plan.Plan#blocksAccessed()
    */
  def blocksAccessed(): Int = {
    p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
  }

  /** Estimates the number of output records in the product. The formula is:
    * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
    * @see
    *   simpledb.plan.Plan#recordsOutput()
    */
  def recordsOutput(): Int = {
    p1.recordsOutput() * p2.recordsOutput();
  }

  /** Estimates the distinct number of field values in the product. Since the
    * product does not increase or decrease field values, the estimate is the
    * same as in the appropriate underlying query.
    * @see
    *   simpledb.plan.Plan#distinctValues(java.lang.String)
    */
  def distinctValues(fldname: String): Int = {
    if (p1.schema().hasField(fldname))
      p1.distinctValues(fldname);
    else
      p2.distinctValues(fldname);
  }

  /** Returns the schema of the product, which is the union of the schemas of
    * the underlying queries.
    * @see
    *   simpledb.plan.Plan#schema()
    */
  def schema(): Schema = {
    _schema;
  }
}
