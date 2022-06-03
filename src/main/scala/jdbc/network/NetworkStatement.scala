package simpledb.jdbc.network;

import java.sql._;
import simpledb.jdbc.StatementAdapter;

/** An adapter class that wraps RemoteStatement. Its methods do nothing except
  * transform RemoteExceptions into SQLExceptions.
  * @author
  *   Edward Sciore
  */
class NetworkStatement(val rstmt: RemoteStatement) {

  def executeQuery(qry: String): NetworkResultSet = {
    try {
      val rrs: RemoteResultSet = rstmt.executeQuery(qry);
      new NetworkResultSet(rrs);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def executeUpdate(cmd: String): Int = {
    try {
      rstmt.executeUpdate(cmd);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def close(): Unit = {
    try {
      rstmt.close();
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }
}
