package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.query._;
import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/** The RMI server-side implementation of RemoteResultSet.
  * @author
  *   Edward Sciore
  */
class RemoteResultSetImpl(val plan: Plan, val rconn: RemoteConnectionImpl) {

  /** Creates a RemoteResultSet object. The specified plan is opened, and the
    * scan is saved.
    * @param plan
    *   the query plan
    * @param rconn
    *   TODO
    * @throws RemoteException
    */
  val s: Scan = plan.open();
  val sch: Schema = plan.schema();

  /** Moves to the next record in the result set, by moving to the next record
    * in the saved scan.
    * @see
    *   simpledb.jdbc.network.RemoteResultSet#next()
    */
  def next(): Boolean = {
    try {
      return s.next();
    } catch {
      case e: RuntimeException => {
        rconn.rollback();
        throw e;
      }
    }
  }

  /** Returns the integer value of the specified field, by returning the
    * corresponding value on the saved scan.
    * @see
    *   simpledb.jdbc.network.RemoteResultSet#getInt(java.lang.String)
    */
  def getInt(_fldname: String): Int = {
    try {
      val fldname = _fldname.toLowerCase(); // to ensure case-insensitivity
      s.getInt(fldname);
    } catch {
      case e: RuntimeException => {
        rconn.rollback();
        throw e;
      }
    }
  }

  /** Returns the integer value of the specified field, by returning the
    * corresponding value on the saved scan.
    * @see
    *   simpledb.jdbc.network.RemoteResultSet#getInt(java.lang.String)
    */
  def getString(_fldname: String): String = {
    try {
      val fldname = _fldname.toLowerCase(); // to ensure case-insensitivity
      s.getString(fldname);
    } catch {
      case e: RuntimeException => {
        rconn.rollback();
        throw e;
      }
    }
  }

  /** Returns the result set's metadata, by passing its schema into the
    * RemoteMetaData constructor.
    * @see
    *   simpledb.jdbc.network.RemoteResultSet#getMetaData()
    */
  def getMetaData(): RemoteMetaDataImpl = {
    new RemoteMetaDataImpl(sch);
  }

  /** Closes the result set by closing its scan.
    * @see
    *   simpledb.jdbc.network.RemoteResultSet#close()
    */
  def close(): Unit = {
    s.close();
    rconn.commit();
  }
}
