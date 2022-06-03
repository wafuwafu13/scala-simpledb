package simpledb.jdbc.embedded;

import java.sql._;
import simpledb.record.Schema;
import simpledb.query.Scan;
import simpledb.plan.Plan;
import simpledb.jdbc.ResultSetAdapter;

/** The embedded implementation of ResultSet.
  * @author
  *   Edward Sciore
  */
// TODO: extends
class EmbeddedResultSet(val plan: Plan, val conn: EmbeddedConnection) {

  /** Creates a Scan object from the specified plan.
    * @param plan
    *   the query plan
    * @param conn
    *   the connection
    * @throws RemoteException
    */
  val s: Scan = plan.open();
  val sch: Schema = plan.schema();

  /** Moves to the next record in the result set, by moving to the next record
    * in the saved scan.
    */
  def next(): Boolean = {
    try {
      return s.next();
    } catch {
      case e: RuntimeException => {
        conn.rollback();
        throw new SQLException(e);
      }

    }
  }

  /** Returns the integer value of the specified field, by returning the
    * corresponding value on the saved scan.
    */
  def getInt(_fldname: String): Int = {
    try {
      val fldname = _fldname.toLowerCase(); // to ensure case-insensitivity
      s.getInt(fldname);
    } catch {
      case e: RuntimeException => {
        conn.rollback();
        throw new SQLException(e);
      }
    }
  }

  /** Returns the integer value of the specified field, by returning the
    * corresponding value on the saved scan.
    */
  def getString(_fldname: String): String = {
    try {
      val fldname = _fldname.toLowerCase(); // to ensure case-insensitivity
      s.getString(fldname);
    } catch {
      case e: RuntimeException => {
        conn.rollback();
        throw new SQLException(e);

      }

    }
  }

  /** Returns the result set's metadata, by passing its schema into the
    * EmbeddedMetaData constructor.
    */

  def getMetaData(): EmbeddedMetaData = {
    return new EmbeddedMetaData(sch);
  }

  /** Closes the result set by closing its scan, and commits.
    */
  def close(): Unit = {
    s.close();
    conn.commit();
  }
}
