package simpledb.jdbc.network;

import java.rmi._;

/** The RMI remote interface corresponding to Connection. The methods are
  * identical to those of Connection, except that they throw RemoteExceptions
  * instead of SQLExceptions.
  * @author
  *   Edward Sciore
  */
trait RemoteConnection {
  def createStatement(): RemoteStatement;
  def close(): Unit;
}
