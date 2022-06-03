package simpledb.jdbc;

import java.sql._;
import java.util._;
import java.util.logging.Logger;

/** This class implements all of the methods of the Driver interface, by
  * throwing an exception for each one. Subclasses (such as SimpleDriver) can
  * override those methods that it want to implement.
  * @author
  *   Edward Sciore
  */
class DriverAdapter extends Driver {
  def acceptsURL(url: String) = {
    // TODO: fix incompatible type in overriding
    new SQLException("operation not implemented").asInstanceOf[Boolean];
  }

  def connect(url: String, info: Properties) = {
    new SQLException("operation not implemented").asInstanceOf[Connection];
  }

  def getMajorVersion(): Int = {
    0;
  }

  def getMinorVersion(): Int = {
    0;
  }

  def getPropertyInfo(
      url: String,
      info: Properties
  ): scala.Array[DriverPropertyInfo] = {
    null;
  }

  def jdbcCompliant(): Boolean = {
    false;
  }

  def getParentLogger(): Logger = {
    throw new SQLFeatureNotSupportedException("operation not implemented");
  }
}
