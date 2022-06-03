package simpledb.jdbc.network;

import java.sql._;
import simpledb.jdbc.ResultSetAdapter;

/** An adapter class that wraps RemoteResultSet. Its methods do nothing except
  * transform RemoteExceptions into SQLExceptions.
  * @author
  *   Edward Sciore
  */
class NetworkResultSet(rrs: RemoteResultSet) {

  def next(): Boolean = {
    try {
      return rrs.next();
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def getInt(fldname: String): Int = {
    try {
      rrs.getInt(fldname);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def getString(fldname: String): String = {
    try {
      return rrs.getString(fldname);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def getMetaData(): NetworkMetaData = {
    try {
      val rmd: RemoteMetaData = rrs.getMetaData();
      return new NetworkMetaData(rmd);
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }

  def close(): Unit = {
    try {
      rrs.close();
    } catch {
      case e: Exception =>
        throw new SQLException(e);
    }
  }
}
