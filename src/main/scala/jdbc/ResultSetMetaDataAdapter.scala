package simpledb.jdbc;

import java.sql._;

/** This class implements all of the methods of the ResultSetMetaData interface,
  * by throwing an exception for each one. Subclasses (such as SimpleMetaData)
  * can override those methods that it want to implement.
  * @author
  *   Edward Sciore
  */
abstract class ResultSetMetaDataAdapter extends ResultSetMetaData {
  def getCatalogName(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getColumnClassName(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getColumnCount(): Int = {
    throw new SQLException("operation not implemented");
  }

  def getColumnDisplaySize(column: Int): Int = {
    throw new SQLException("operation not implemented");
  }

  def getColumnLabel(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getColumnName(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getColumnType(column: Int): Int = {
    throw new SQLException("operation not implemented");
  }

  def getColumnTypeName(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getPrecision(column: Int): Int = {
    throw new SQLException("operation not implemented");
  }

  def getScale(column: Int): Int = {
    throw new SQLException("operation not implemented");
  }

  def getSchemaName(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getTableName(column: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def isAutoIncrement(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isCaseSensitive(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isCurrency(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isDefinitelyWritable(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isNullable(column: Int): Int = {
    throw new SQLException("operation not implemented");
  }

  def isReadOnly(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isSearchable(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isSigned(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isWritable(column: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def isWrapperFor(iface: Class[Any]): Unit = {
    throw new SQLException("operation not implemented");
  }

  def unwrap(iface: Class[Any]): Unit = {
    throw new SQLException("operation not implemented");
  }
}
