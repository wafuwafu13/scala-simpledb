package simpledb.jdbc.network;

import java.rmi._;

/** The RMI remote interface corresponding to ResultSet. The methods are
  * identical to those of ResultSet, except that they throw RemoteExceptions
  * instead of SQLExceptions.
  * @author
  *   Edward Sciore
  */
trait RemoteResultSet extends Remote {
  def next(): Boolean;
  def getInt(fldname: String): Int;
  def getString(fldname: String): String;
  def getMetaData(): RemoteMetaData;
  def close(): Unit;
}
