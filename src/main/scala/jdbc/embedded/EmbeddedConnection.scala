package simpledb.jdbc.embedded;

import java.sql.SQLException;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.jdbc.ConnectionAdapter;

/** The embedded implementation of Connection.
  * @author
  *   Edward Sciore
  */

class EmbeddedConnection(val db: SimpleDB) {

  /** Creates a connection and begins a new transaction for it.
    * @throws RemoteException
    */

  var currentTx: Transaction = db.newTx();
  val planner: Planner = db.getPlanner();

  /** Creates a new Statement for this connection.
    */
  def createStatement(): EmbeddedStatement = {
    new EmbeddedStatement(this, planner);
  }

  /** Closes the connection by committing the current transaction.
    */
  def close(): Unit = {
    currentTx.commit();
  }

  /** Commits the current transaction and begins a new one.
    */
  def commit(): Unit = {
    currentTx.commit();
    currentTx = db.newTx();
  }

  /** Rolls back the current transaction and begins a new one.
    */
  def rollback(): Unit = {
    currentTx.rollback();
    currentTx = db.newTx();
  }

  /** Returns the transaction currently associated with this connection. Not
    * public. Called by other JDBC classes.
    * @return
    *   the transaction associated with this connection
    */
  def getTransaction(): Transaction = {
    currentTx;
  }
}
