package simpledb.jdbc.network;

import java.rmi._;

/** The RMI remote interface corresponding to ResultSetMetaData. The methods are
  * identical to those of ResultSetMetaData, except that they throw
  * RemoteExceptions instead of SQLExceptions.
  * @author
  *   Edward Sciore
  */
trait RemoteMetaData extends Remote {
  def getColumnCount(): Int;
  def getColumnName(column: Int): String;
  def getColumnType(column: Int): Int;
  def getColumnDisplaySize(column: Int): Int;
}
