package simpledb.jdbc;

import java.sql._;
import java.util.Map;
import java.util.Calendar;
import java.io._;
import java.math.BigDecimal;
import java.net.URL;

/** This class implements all of the methods of the ResultSet interface, by
  * throwing an exception for each one. Subclasses (such as SimpleResultSet) can
  * override those methods that it want to implement.
  * @author
  *   Edward Sciore
  */
abstract class ResultSetAdapter extends ResultSet {
  def absolute(row: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def afterLast() = {
    throw new SQLException("operation not implemented");
  }

  def beforeFirst() = {
    throw new SQLException("operation not implemented");
  }

  def cancelRowUpdates() = {
    throw new SQLException("operation not implemented");
  }

  def clearWarnings() = {
    throw new SQLException("operation not implemented");
  }

  def close() = {
    throw new SQLException("operation not implemented");
  }

  def deleteRow() = {
    throw new SQLException("operation not implemented");
  }

  def findColumn(columnLabel: String): Int = {
    throw new SQLException("operation not implemented");
  }

  def first(): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def getArray(columnIndex: Int): Array = {
    throw new SQLException("operation not implemented");
  }

  def getArray(columnLabel: String): Array = {
    throw new SQLException("operation not implemented");
  }

  def getAsciiStream(columnIndex: Int): InputStream = {
    throw new SQLException("operation not implemented");
  }

  def getAsciiStream(columnLabel: String): InputStream = {
    throw new SQLException("operation not implemented");
  }

  def getBigDecimal(columnIndex: Int): java.math.BigDecimal = {
    throw new SQLException("operation not implemented");
  }

  def getBigDecimal(columnLabel: String): java.math.BigDecimal = {
    throw new SQLException("operation not implemented");
  }

  def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = {
    throw new SQLException("operation not implemented");
  }

  def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = {
    throw new SQLException("operation not implemented");
  }

  def getBinaryStream(columnIndex: Int): java.io.InputStream = {
    throw new SQLException("operation not implemented");
  }

  def getBinaryStream(columnLabel: String): java.io.InputStream = {
    throw new SQLException("operation not implemented");
  }

  def getBlob(columnIndex: Int): Blob = {
    throw new SQLException("operation not implemented");
  }

  def getBlob(columnLabel: String): Blob = {
    throw new SQLException("operation not implemented");
  }

  def getBoolean(columnIndex: Int): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def getBoolean(columnLabel: String): Boolean = {
    throw new SQLException("operation not implemented");
  }

  def getByte(columnIndex: Int): Byte = {
    throw new SQLException("operation not implemented");
  }

  def getByte(columnLabel: String): Byte = {
    throw new SQLException("operation not implemented");
  }

  def getBytes(columnIndex: Int): scala.Array[Byte] = {
    throw new SQLException("operation not implemented");
  }

  def getBytes(columnLabel: String): scala.Array[Byte] = {
    throw new SQLException("operation not implemented");
  }

  def getCharacterStream(columnIndex: Int): Reader = {
    throw new SQLException("operation not implemented");
  }

  def getCharacterStream(columnLabel: String): Reader = {
    throw new SQLException("operation not implemented");
  }

  def getClob(columnIndex: Int): Clob = {
    throw new SQLException("operation not implemented");
  }

  def getClob(columnLabel: String): Clob = {
    throw new SQLException("operation not implemented");
  }

  def getConcurrency(): Int = {
    throw new SQLException("operation not implemented");
  }

  def getCursorName(): String = {
    throw new SQLException("operation not implemented");
  }

  def getDate(columnIndex: Int): Date = {
    throw new SQLException("operation not implemented");
  }

  def getDate(columnLabel: String): Date = {
    throw new SQLException("operation not implemented");
  }

  def getDate(columnIndex: Int, cal: Calendar): Date = {
    throw new SQLException("operation not implemented");
  }

  def getDate(columnLabel: String, cal: Calendar): Date = {
    throw new SQLException("operation not implemented");
  }

  def getDouble(columnIndex: Int): Double = {
    throw new SQLException("operation not implemented");
  }

  def getDouble(columnLabel: String): Double = {
    throw new SQLException("operation not implemented");
  }

  def getFetchDirection(): Int = {
    throw new SQLException("operation not implemented");
  }

  def getFetchSize(): Int = {
    throw new SQLException("operation not implemented");
  }

  def getFloat(columnIndex: Int): Float = {
    throw new SQLException("operation not implemented");
  }

  def getFloat(columnLabel: String): Float = {
    throw new SQLException("operation not implemented");
  }

  def getHoldability(): Int = {
    throw new SQLException("operation not implemented");
  }

  def getInt(columnIndex: Int): Int = {
    throw new SQLException("operation not implemented");
  }

  def getInt(columnLabel: String): Int = {
    throw new SQLException("operation not implemented");
  }

  def getLong(columnIndex: Int): Long = {
    throw new SQLException("operation not implemented");
  }

  def getLong(columnLabel: String): Long = {
    throw new SQLException("operation not implemented");
  }

  def getMetaData(): ResultSetMetaData = {
    throw new SQLException("operation not implemented");
  }

  def getNCharacterStream(columnIndex: Int): Reader = {
    throw new SQLException("operation not implemented");
  }

  def getNCharacterStream(columnLabel: String): Reader = {
    throw new SQLException("operation not implemented");
  }

  def getNClob(columnIndex: Int): NClob = {
    throw new SQLException("operation not implemented");
  }

  def getNClob(columnLabel: String): NClob = {
    throw new SQLException("operation not implemented");
  }

  def getNString(columnIndex: Int): String = {
    throw new SQLException("operation not implemented");
  }

  def getNString(columnLabel: String): String = {
    throw new SQLException("operation not implemented");
  }

  def getObject(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getObject(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getObject(columnIndex: Int, map: Map[String, Class[Any]]) = {
    throw new SQLException("operation not implemented");
  }

  def getObject(columnLabel: String, map: Map[String, Class[Any]]) = {
    throw new SQLException("operation not implemented");
  }

  def getRef(columnIndex: Int): Ref = {
    throw new SQLException("operation not implemented");
  }

  def getRef(columnLabel: String): Ref = {
    throw new SQLException("operation not implemented");
  }

  def getRow(): Int = {
    throw new SQLException("operation not implemented");
  }

  def getRowId(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getRowId(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getShort(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getShort(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getSQLXML(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getSQLXML(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getStatement() = {
    throw new SQLException("operation not implemented");
  }

  def getString(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getString(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getTime(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getTime(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getTime(columnIndex: Int, cal: Calendar) = {
    throw new SQLException("operation not implemented");
  }

  def getTime(columnLabel: String, cal: Calendar) = {
    throw new SQLException("operation not implemented");
  }

  def getTimestamp(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getTimestamp(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getTimestamp(columnIndex: Int, cal: Calendar) = {
    throw new SQLException("operation not implemented");
  }

  def getTimestamp(columnLabel: String, cal: Calendar) = {
    throw new SQLException("operation not implemented");
  }

  def getType() = {
    throw new SQLException("operation not implemented");
  }

  def getUnicodeStream(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getUnicodeStream(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getURL(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def getURL(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def getWarnings() = {
    throw new SQLException("operation not implemented");
  }

  def insertRow() = {
    throw new SQLException("operation not implemented");
  }

  def isAfterLast() = {
    throw new SQLException("operation not implemented");
  }

  def isBeforeFirst() = {
    throw new SQLException("operation not implemented");
  }

  def isClosed() = {
    throw new SQLException("operation not implemented");
  }

  def isFirst() = {
    throw new SQLException("operation not implemented");
  }

  def isLast() = {
    throw new SQLException("operation not implemented");
  }

  def last() = {
    throw new SQLException("operation not implemented");
  }

  def moveToCurrentRow() = {
    throw new SQLException("operation not implemented");
  }

  def moveToInsertRow() = {
    throw new SQLException("operation not implemented");
  }

  def next() = {
    throw new SQLException("operation not implemented");
  }

  def previous() = {
    throw new SQLException("operation not implemented");
  }

  def refreshRow() = {
    throw new SQLException("operation not implemented");
  }

  def relative(rows: Int) = {
    throw new SQLException("operation not implemented");
  }

  def rowDeleted() = {
    throw new SQLException("operation not implemented");
  }

  def rowInserted() = {
    throw new SQLException("operation not implemented");
  }

  def rowUpdated() = {
    throw new SQLException("operation not implemented");
  }

  def setFetchDirection(direction: Int) = {
    throw new SQLException("operation not implemented");
  }

  def setFetchSize(rows: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateArray(columnIndex: Int, x: Array) = {
    throw new SQLException("operation not implemented");
  }

  def updateArray(columnLabel: String, x: Array) = {
    throw new SQLException("operation not implemented");
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream) = {
    throw new SQLException("operation not implemented");
  }

  def updateAsciiStream(columnLabel: String, x: InputStream) = {
    throw new SQLException("operation not implemented");
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateAsciiStream(columnLabel: String, x: InputStream, length: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateAsciiStream(columnLabel: String, x: InputStream, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateBigDecimal(columnIndex: Int, x: BigDecimal) = {
    throw new SQLException("operation not implemented");
  }

  def updateBigDecimal(columnLabel: String, x: BigDecimal) = {
    throw new SQLException("operation not implemented");
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream) = {
    throw new SQLException("operation not implemented");
  }

  def updateBinaryStream(columnLabel: String, x: InputStream) = {
    throw new SQLException("operation not implemented");
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateBinaryStream(columnLabel: String, x: InputStream, length: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateBinaryStream(columnLabel: String, x: InputStream, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateBlob(columnIndex: Int, x: Blob) = {
    throw new SQLException("operation not implemented");
  }

  def updateBlob(columnLabel: String, x: Blob) = {
    throw new SQLException("operation not implemented");
  }

  def updateBlob(columnIndex: Int, inputStream: InputStream) = {
    throw new SQLException("operation not implemented");
  }

  def updateBlob(columnLabel: String, inputStream: InputStream) = {
    throw new SQLException("operation not implemented");
  }

  def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateBlob(
      columnLabel: String,
      inputStream: InputStream,
      length: Long
  ) = {
    throw new SQLException("operation not implemented");
  }

  def updateBoolean(columnIndex: Int, x: Boolean) = {
    throw new SQLException("operation not implemented");
  }

  def updateBoolean(columnLabel: String, x: Boolean) = {
    throw new SQLException("operation not implemented");
  }

  def updateByte(columnIndex: Int, x: Byte) = {
    throw new SQLException("operation not implemented");
  }

  def updateByte(columnLabel: String, x: Byte) = {
    throw new SQLException("operation not implemented");
  }

  def updateBytes(columnIndex: Int, x: scala.Array[Byte]) = {
    throw new SQLException("operation not implemented");
  }

  def updateBytes(columnLabel: String, x: scala.Array[Byte]) = {
    throw new SQLException("operation not implemented");
  }

  def updateCharacterStream(columnIndex: Int, x: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateCharacterStream(columnLabel: String, x: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateCharacterStream(columnIndex: Int, x: Reader, length: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateCharacterStream(columnLabel: String, x: Reader, length: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateCharacterStream(columnIndex: Int, x: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateCharacterStream(columnLabel: String, x: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateClob(columnIndex: Int, x: Clob) = {
    throw new SQLException("operation not implemented");
  }

  def updateClob(columnLabel: String, x: Clob) = {
    throw new SQLException("operation not implemented");
  }

  def updateClob(columnIndex: Int, reader: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateClob(columnLabel: String, reader: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateClob(columnIndex: Int, reader: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateClob(columnLabel: String, reader: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateDate(columnIndex: Int, x: Date) = {
    throw new SQLException("operation not implemented");
  }

  def updateDate(columnLabel: String, x: Date) = {
    throw new SQLException("operation not implemented");
  }

  def updateDouble(columnIndex: Int, x: Double) = {
    throw new SQLException("operation not implemented");
  }

  def updateDouble(columnLabel: String, x: Double) = {
    throw new SQLException("operation not implemented");
  }

  def updateFloat(columnIndex: Int, x: Float) = {
    throw new SQLException("operation not implemented");
  }

  def updateFloat(columnLabel: String, x: Float) = {
    throw new SQLException("operation not implemented");
  }

  def updateInt(columnIndex: Int, x: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateInt(columnLabel: String, x: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateLong(columnIndex: Int, x: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateLong(columnLabel: String, x: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateNCharacterStream(columnIndex: Int, x: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateNCharacterStream(columnLabel: String, x: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateNCharacterStream(columnLabel: String, x: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateNClob(columnIndex: Int, nclob: NClob) = {
    throw new SQLException("operation not implemented");
  }

  def updateNClob(columnLabel: String, nclob: NClob) = {
    throw new SQLException("operation not implemented");
  }

  def updateNClob(columnIndex: Int, reader: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateNClob(columnLabel: String, reader: Reader) = {
    throw new SQLException("operation not implemented");
  }

  def updateNClob(columnIndex: Int, reader: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateNClob(columnLabel: String, reader: Reader, length: Long) = {
    throw new SQLException("operation not implemented");
  }

  def updateNString(columnIndex: Int, string: String) = {
    throw new SQLException("operation not implemented");
  }

  def updateNString(columnLabel: String, string: String) = {
    throw new SQLException("operation not implemented");
  }

  def updateNull(columnIndex: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateNull(columnLabel: String) = {
    throw new SQLException("operation not implemented");
  }

  def updateObject(columnIndex: Int, x: Object) = {
    throw new SQLException("operation not implemented");
  }

  def updateObject(columnLabel: String, x: Object) = {
    throw new SQLException("operation not implemented");
  }

  def updateObject(columnIndex: Int, x: Object, scale: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateObject(columnLabel: String, x: Object, scale: Int) = {
    throw new SQLException("operation not implemented");
  }

  def updateRef(columnIndex: Int, x: Ref) = {
    throw new SQLException("operation not implemented");
  }

  def updateRef(columnLabel: String, x: Ref) = {
    throw new SQLException("operation not implemented");
  }

  def updateRow() = {
    throw new SQLException("operation not implemented");
  }

  def updateRowId(columnIndex: Int, x: RowId) = {
    throw new SQLException("operation not implemented");
  }

  def updateRowId(columnLabel: String, x: RowId) = {
    throw new SQLException("operation not implemented");
  }

  def updateShort(columnIndex: Int, x: Short) = {
    throw new SQLException("operation not implemented");
  }

  def updateShort(columnLabel: String, x: Short) = {
    throw new SQLException("operation not implemented");
  }

  def updateSQLXML(columnIndex: Int, x: SQLXML) = {
    throw new SQLException("operation not implemented");
  }

  def updateSQLXML(columnLabel: String, x: SQLXML) = {
    throw new SQLException("operation not implemented");
  }

  def updateString(columnIndex: Int, x: String) = {
    throw new SQLException("operation not implemented");
  }

  def updateString(columnLabel: String, x: String) = {
    throw new SQLException("operation not implemented");
  }

  def updateTime(columnIndex: Int, x: Time) = {
    throw new SQLException("operation not implemented");
  }

  def updateTime(columnLabel: String, x: Time) = {
    throw new SQLException("operation not implemented");
  }

  def updateTimestamp(columnIndex: Int, x: Timestamp) = {
    throw new SQLException("operation not implemented");
  }

  def updateTimestamp(columnLabel: String, x: Timestamp) = {
    throw new SQLException("operation not implemented");
  }

  def wasNull() = {
    throw new SQLException("operation not implemented");
  }

  def isWrapperFor(iface: Class[Any]) = {
    throw new SQLException("operation not implemented");
  }

  def unwrap(iface: Class[Any]) = {
    throw new SQLException("operation not implemented");
  }

  def getObject(columnIndex: Int, typeval: Class[Any]) = {
    throw new SQLException("operation not implemented");
  }

  def getObject(columnLabel: String, typeval: Class[Any]) = {
    throw new SQLException("operation not implemented");
  }
}
