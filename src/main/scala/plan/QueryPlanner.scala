package simpledb.plan;

import simpledb.tx.Transaction;
import simpledb.parse.QueryData;

/** The interface implemented by planners for the SQL select statement.
  * @author
  *   Edward Sciore
  */
trait QueryPlanner {

  /** Creates a plan for the parsed query.
    * @param data
    *   the parsed representation of the query
    * @param tx
    *   the calling transaction
    * @return
    *   a plan for that query
    */
  def createPlan(data: QueryData, tx: Transaction): Plan;
}
