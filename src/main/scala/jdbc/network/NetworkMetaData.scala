package simpledb.jdbc.network;

import java.sql._;
import simpledb.jdbc.ResultSetMetaDataAdapter;

/** An adapter class that wraps RemoteMetaData. Its methods do nothing except
  * transform RemoteExceptions into SQLExceptions.
  * @author
  *   Edward Sciore
  */
class NetworkMetaData(rmd: RemoteMetaData) {

  def getColumnCount(): Int = {
    try {
      rmd.getColumnCount();
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def getColumnName(column: Int): String = {
    try {
      rmd.getColumnName(column);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def getColumnType(column: Int): Int = {
    try {
      rmd.getColumnType(column);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def getColumnDisplaySize(column: Int): Int = {
    try {
      rmd.getColumnDisplaySize(column);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }
}
