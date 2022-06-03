package simpledb.jdbc.network;

import java.rmi._;

/** The RMI remote interface corresponding to Driver. The method is similar to
  * that of Driver, except that it takes no arguments and throws
  * RemoteExceptions instead of SQLExceptions.
  * @author
  *   Edward Sciore
  */
trait RemoteDriver extends Remote {
  def connect(): RemoteConnection;
}
