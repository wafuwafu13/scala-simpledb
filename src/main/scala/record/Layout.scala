package simpledb.record;

import java.util._;
import java.sql.Types._;
import simpledb.file.Page;
import collection.JavaConverters._;
import java.nio.charset._;

/** Description of the structure of a record. It contains the name, type, length
  * and offset of each field of the table.
  * @author
  *   Edward Sciore
  */
class Layout(val schema: Schema) {

  /** This constructor creates a Layout object from a schema. This constructor
    * is used when a table is created. It determines the physical offset of each
    * field within the record.
    * @param tblname
    *   the name of the table
    * @param schema
    *   the schema of the table's records
    */
  private val offsets: Map[String, Integer] =
    scala.collection.mutable.HashMap().asJava;
  var pos = Integer.BYTES // leave space for the empty/inuse flag
  schema
    .getFields()
    .forEach(fldname => {
      offsets.put(fldname, pos);
      pos += lengthInBytes(fldname);
    })

  private val slotsize: Int = pos;

  /** Return the schema of the table's records
    * @return
    *   the table's record schema
    */
  def getSchema(): Schema = {
    schema;
  }

  /** Return the offset of a specified field within a record
    * @param fldname
    *   the name of the field
    * @return
    *   the offset of that field within a record
    */
  def offset(fldname: String): Int = {
    offsets.get(fldname);
  }

  /** Return the size of a slot, in bytes.
    * @return
    *   the size of a slot
    */
  def slotSize(): Int = {
    slotsize;
  }

  private def lengthInBytes(fldname: String): Int = {
    val fldtype: Int = schema.getType(fldname);
    if (fldtype == INTEGER) {
      Integer.BYTES;
    } else // fldtype == VARCHAR
      maxLength(schema.length(fldname));
  }

  def maxLength(strlen: Int): Int = {
    val bytesPerChar: Float =
      StandardCharsets.US_ASCII.newEncoder().maxBytesPerChar();
    Integer.BYTES + (strlen * bytesPerChar.asInstanceOf[Int]);
  }
}
