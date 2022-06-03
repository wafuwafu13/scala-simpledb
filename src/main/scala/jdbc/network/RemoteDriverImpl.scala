package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import simpledb.server.SimpleDB;

/** The RMI server-side implementation of RemoteDriver.
  * @author
  *   Edward Sciore
  */
class RemoteDriverImpl(val db: SimpleDB) {

  /** Creates a new RemoteConnectionImpl object and returns it.
    * @see
    *   simpledb.jdbc.network.RemoteDriver#connect()
    */
  def connect(): RemoteConnectionImpl = {
    new RemoteConnectionImpl(db);
  }
}
