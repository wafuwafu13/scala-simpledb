package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/** The RMI server-side implementation of RemoteStatement.
  * @author
  *   Edward Sciore
  */
class RemoteStatementImpl(
    val rconn: RemoteConnectionImpl,
    val planner: Planner
) {

  /** Executes the specified SQL query string. The method calls the query
    * planner to create a plan for the query. It then sends the plan to the
    * RemoteResultSetImpl constructor for processing.
    * @see
    *   simpledb.jdbc.network.RemoteStatement#executeQuery(java.lang.String)
    */
  def executeQuery(qry: String): RemoteResultSetImpl = {
    try {
      val tx: Transaction = rconn.getTransaction();
      val pln: Plan = planner.createQueryPlan(qry, tx);
      return new RemoteResultSetImpl(pln, rconn);
    } catch {
      case e: RuntimeException => {
        rconn.rollback();
        throw e;
      }
    }
  }

  /** Executes the specified SQL update command. The method sends the command to
    * the update planner, which executes it.
    * @see
    *   simpledb.jdbc.network.RemoteStatement#executeUpdate(java.lang.String)
    */
  def executeUpdate(cmd: String): Int = {
    try {
      val tx: Transaction = rconn.getTransaction();
      val result: Int = planner.executeUpdate(cmd, tx);
      rconn.commit();
      result;
    } catch {
      case e: RuntimeException => {
        rconn.rollback();
        throw e;
      }
    }
  }

  def close(): Unit = {}
}
