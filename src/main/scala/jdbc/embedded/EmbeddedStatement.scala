package simpledb.jdbc.embedded;

import java.sql.SQLException;
import simpledb.tx.Transaction;
import simpledb.plan._;
import simpledb.jdbc.StatementAdapter;

/** The embedded implementation of Statement.
  * @author
  *   Edward Sciore
  */
class EmbeddedStatement(conn: EmbeddedConnection, planner: Planner) {

  /** Executes the specified SQL query string. Calls the query planner to create
    * a plan for the query, and sends the plan to the ResultSet constructor for
    * processing. Rolls back and throws an SQLException if it cannot create the
    * plan.
    */
  def executeQuery(qry: String): EmbeddedResultSet = {
    try {
      val tx: Transaction = conn.getTransaction();
      val pln: Plan = planner.createQueryPlan(qry, tx);
      return new EmbeddedResultSet(pln, conn);
    } catch {
      case e: RuntimeException => {
        conn.rollback();
        throw new SQLException(e);
      }

    }
  }

  /** Executes the specified SQL update command by sending the command to the
    * update planner and then committing. Rolls back and throws an SQLException
    * on an error.
    */
  def executeUpdate(cmd: String): Int = {
    try {
      val tx: Transaction = conn.getTransaction();
      val result: Int = planner.executeUpdate(cmd, tx);
      conn.commit();
      result;
    } catch {
      case e: RuntimeException => {
        conn.rollback();
        throw new SQLException(e);
      }
    }
  }

  def close(): Unit = {}
}
