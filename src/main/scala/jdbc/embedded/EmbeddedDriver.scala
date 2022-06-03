package simpledb.jdbc.embedded;

import java.util.Properties;
import java.sql.SQLException;
import simpledb.server.SimpleDB;
import simpledb.jdbc.DriverAdapter;

/** The RMI server-side implementation of RemoteDriver.
  * @author
  *   Edward Sciore
  */

class EmbeddedDriver extends DriverAdapter {

  /** Creates a new RemoteConnectionImpl object and returns it.
    * @see
    *   simpledb.jdbc.network.RemoteDriver#connect()
    */
  override def connect(url: String, p: Properties): java.sql.Connection = {
    val dbname: String = url.replace("jdbc:simpledb:", "");
    val db: SimpleDB = new SimpleDB(dbname, null, null);
    // TODO: remove asInstanceOf
    new EmbeddedConnection(db).asInstanceOf[java.sql.Connection];
  }
}
