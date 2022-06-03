package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/** The RMI server-side implementation of RemoteConnection.
  * @author
  *   Edward Sciore
  */

class RemoteConnectionImpl(val db: SimpleDB) {

  /** Creates a remote connection and begins a new transaction for it.
    * @throws RemoteException
    */

  var currentTx: Transaction = db.newTx();
  val planner: Planner = db.getPlanner();

  /** Creates a new RemoteStatement for this connection.
    * @see
    *   simpledb.jdbc.network.RemoteConnection#createStatement()
    */
  def createStatement(): RemoteStatementImpl = {
    new RemoteStatementImpl(this, planner);
  }

  /** Closes the connection. The current transaction is committed.
    * @see
    *   simpledb.jdbc.network.RemoteConnection#close()
    */
  def close(): Unit = {
    currentTx.commit();
  }

// The following methods are used by the server-side classes.

  /** Returns the transaction currently associated with this connection.
    * @return
    *   the transaction associated with this connection
    */
  def getTransaction(): Transaction = {
    currentTx;
  }

  /** Commits the current transaction, and begins a new one.
    */
  def commit(): Unit = {
    currentTx.commit();
    currentTx = db.newTx();
  }

  /** Rolls back the current transaction, and begins a new one.
    */
  def rollback(): Unit = {
    currentTx.rollback();
    currentTx = db.newTx();
  }
}
