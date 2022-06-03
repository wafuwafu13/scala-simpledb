package simpledb.jdbc.network;

import simpledb.record.Schema;
import java.sql.Types.INTEGER;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util._;

/** The RMI server-side implementation of RemoteMetaData.
  * @author
  *   Edward Sciore
  */
class RemoteMetaDataImpl(val sch: Schema) {

  /** Creates a metadata object that wraps the specified schema. The method also
    * creates a list to hold the schema's collection of field names, so that the
    * fields can be accessed by position.
    * @param sch
    *   the schema
    * @throws RemoteException
    */
  val fields: List[String] = new ArrayList[String]();
  sch
    .getFields()
    .forEach(fld => {
      fields.add(fld);
    })

  /** Returns the size of the field list.
    * @see
    *   simpledb.jdbc.network.RemoteMetaData#getColumnCount()
    */
  def getColumnCount(): Int = {
    fields.size();
  }

  /** Returns the field name for the specified column number. In JDBC, column
    * numbers start with 1, so the field is taken from position (column-1) in
    * the list.
    * @see
    *   simpledb.jdbc.network.RemoteMetaData#getColumnName(int)
    */
  def getColumnName(column: Int): String = {
    fields.get(column - 1);
  }

  /** Returns the type of the specified column. The method first finds the name
    * of the field in that column, and then looks up its type in the schema.
    * @see
    *   simpledb.jdbc.network.RemoteMetaData#getColumnType(int)
    */
  def getColumnType(column: Int): Int = {
    val fldname: String = getColumnName(column);
    sch.getType(fldname);
  }

  /** Returns the number of characters required to display the specified column.
    * For a string-type field, the method simply looks up the field's length in
    * the schema and returns that. For an int-type field, the method needs to
    * decide how large integers can be. Here, the method arbitrarily chooses 6
    * characters, which means that integers over 999,999 will probably get
    * displayed improperly.
    * @see
    *   simpledb.jdbc.network.RemoteMetaData#getColumnDisplaySize(int)
    */
  def getColumnDisplaySize(column: Int): Int = {
    val fldname: String = getColumnName(column);
    val fldtype: Int = sch.getType(fldname);
    val fldlength = if (fldtype == INTEGER) 6 else sch.length(fldname);
    return Math.max(fldname.length(), fldlength) + 1;
  }
}
