package simpledb.jdbc.network;

import java.sql._;
import simpledb.jdbc.ConnectionAdapter;

/** An adapter class that wraps RemoteConnection. Its methods do nothing except
  * transform RemoteExceptions into SQLExceptions.
  * @author
  *   Edward Sciore
  */
class NetworkConnection(rconn: RemoteConnection) {

  def createStatement(): NetworkStatement = {
    try {
      val rstmt: RemoteStatement = rconn.createStatement();
      new NetworkStatement(rstmt);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def close(): Unit = {
    try {
      rconn.close();
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }
}
