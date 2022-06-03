package simpledb.jdbc.network;

import java.rmi._;

/** The RMI remote interface corresponding to Statement. The methods are
  * identical to those of Statement, except that they throw RemoteExceptions
  * instead of SQLExceptions.
  * @author
  *   Edward Sciore
  */
trait RemoteStatement {
  def executeQuery(qry: String): RemoteResultSet;
  def executeUpdate(cmd: String): Int;
  def close(): Unit;
}
