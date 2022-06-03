package simpledb.jdbc.network;

import java.rmi.registry._;
import java.sql._;
import java.util.Properties;
import simpledb.jdbc.DriverAdapter;

/** The SimpleDB database driver.
  * @author
  *   Edward Sciore
  */
class NetworkDriver {

  /** Connects to the SimpleDB server on the specified host. The method
    * retrieves the RemoteDriver stub from the RMI registry on the specified
    * host. It then calls the connect method on that stub, which in turn creates
    * a new connection and returns the RemoteConnection stub for it. This stub
    * is wrapped in a SimpleConnection object and is returned. <P> The current
    * implementation of this method ignores the properties argument.
    * @see
    *   java.sql.Driver#connect(java.lang.String, Properties)
    */
  def connect(url: String, prop: Properties): NetworkConnection = {
    try {
      val host: String =
        url.replace("jdbc:simpledb://", ""); // assumes no port specified
      val reg: Registry = LocateRegistry.getRegistry(host, 1099);
      val rdvr: RemoteDriver =
        reg.lookup("simpledb").asInstanceOf[RemoteDriver];
      val rconn: RemoteConnection = rdvr.connect();
      new NetworkConnection(rconn);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }
}
