package simpledb.jdbc.embedded;

import java.sql.SQLException;
import java.sql.Types.INTEGER;
import simpledb.record.Schema;
import simpledb.jdbc.ResultSetMetaDataAdapter;

/** The embedded implementation of ResultSetMetaData.
  * @author
  *   Edward Sciore
  */
/** Creates a metadata object that wraps the specified schema. The method also
  * creates a list to hold the schema's collection of field names, so that the
  * fields can be accessed by position.
  * @param sch
  *   the schema
  */
class EmbeddedMetaData(val sch: Schema) {

  /** Returns the size of the field list.
    */
  def getColumnCount(): Int = {
    sch.getFields().size();
  }

  /** Returns the field name for the specified column number. In JDBC, column
    * numbers start with 1, so the field is taken from position (column-1) in
    * the list.
    */
  def getColumnName(column: Int): String = {
    sch.getFields().get(column - 1);
  }

  /** Returns the type of the specified column. The method first finds the name
    * of the field in that column, and then looks up its type in the schema.
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
    */
  def getColumnDisplaySize(column: Int): Int = {
    val fldname: String = getColumnName(column);
    val fldtype: Int = sch.getType(fldname);
    val fldlength: Int = if (fldtype == INTEGER) 6 else sch.length(fldname);
    Math.max(fldname.length(), fldlength) + 1;
  }
}
