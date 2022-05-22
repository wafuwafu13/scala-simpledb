package simpledb.plan;

import simpledb.tx.Transaction;
import simpledb.parse._;

/** The object that executes SQL statements.
  * @author
  *   Edward Sciore
  */
class Planner(val qplanner: QueryPlanner, val uplanner: UpdatePlanner) {

  /** Creates a plan for an SQL select statement, using the supplied planner.
    * @param qry
    *   the SQL query string
    * @param tx
    *   the transaction
    * @return
    *   the scan corresponding to the query plan
    */
  def createQueryPlan(qry: String, tx: Transaction): Plan = {
    val parser: Parser = new Parser(qry);
    val data: QueryData = parser.query();
    verifyQuery(data);
    qplanner.createPlan(data, tx);
  }

  /** Executes an SQL insert, delete, modify, or create statement. The method
    * dispatches to the appropriate method of the supplied update planner,
    * depending on what the parser returns.
    * @param cmd
    *   the SQL update string
    * @param tx
    *   the transaction
    * @return
    *   an integer denoting the number of affected records
    */
  def executeUpdate(cmd: String, tx: Transaction): Int = {
    val parser: Parser = new Parser(cmd);
    val data: Object = parser.updateCmd();
    verifyUpdate(data);
    if (data.isInstanceOf[InsertData])
      return uplanner.executeInsert(data.asInstanceOf[InsertData], tx);
    else if (data.isInstanceOf[DeleteData])
      return uplanner.executeDelete(data.asInstanceOf[DeleteData], tx);
    else if (data.isInstanceOf[ModifyData])
      return uplanner.executeModify(data.asInstanceOf[ModifyData], tx);
    else if (data.isInstanceOf[CreateTableData])
      return uplanner.executeCreateTable(
        data.asInstanceOf[CreateTableData],
        tx
      );
    else if (data.isInstanceOf[CreateViewData])
      return uplanner.executeCreateView(data.asInstanceOf[CreateViewData], tx);
    else if (data.isInstanceOf[CreateIndexData])
      return uplanner.executeCreateIndex(
        data.asInstanceOf[CreateIndexData],
        tx
      );
    else
      0;
  }

  // SimpleDB does not verify queries, although it should.
  private def verifyQuery(data: QueryData): Unit = {}

  // SimpleDB does not verify updates, although it should.
  private def verifyUpdate(data: Object): Unit = {}
}
