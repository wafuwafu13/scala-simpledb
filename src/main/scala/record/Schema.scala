package simpledb.record;

import java.sql.Types._;
import java.util._;
import collection.JavaConverters._;

/** The record schema of a table. A schema contains the name and type of each
  * field of the table, as well as the length of each varchar field.
  * @author
  *   Edward Sciore
  */
class Schema {
  private val fields: java.util.List[String] = new java.util.ArrayList[String]()
  private val info: Map[String, FieldInfo] =
    scala.collection.mutable.HashMap().asJava;

  /** Add a field to the schema having a specified name, type, and length. If
    * the field type is "integer", then the length value is irrelevant.
    * @param fldname
    *   the name of the field
    * @param fieldtype
    *   the type of the field, according to the constants in simpledb.sql.types
    * @param length
    *   the conceptual length of a string field.
    */
  def addField(fldname: String, fieldtype: Int, length: Int): Unit = {
    fields.add(fldname);
    info.put(fldname, new FieldInfo(fieldtype, length));
  }

  /** Add an integer field to the schema.
    * @param fldname
    *   the name of the field
    */
  def addIntField(fldname: String): Unit = {
    addField(fldname, INTEGER, 0);
  }

  /** Add a string field to the schema. The length is the conceptual length of
    * the field. For example, if the field is defined as varchar(8), then its
    * length is 8.
    * @param fldname
    *   the name of the field
    * @param length
    *   the number of chars in the varchar definition
    */
  def addStringField(fldname: String, length: Int) = {
    addField(fldname, VARCHAR, length);
  }

  /** Add a field to the schema having the same type and length as the
    * corresponding field in another schema.
    * @param fldname
    *   the name of the field
    * @param sch
    *   the other schema
    */
  def add(fldname: String, sch: Schema): Unit = {
    val fieldtype: Int = sch.getType(fldname);
    val length: Int = sch.length(fldname);
    addField(fldname, fieldtype, length);
  }

  /** Add all of the fields in the specified schema to the current schema.
    * @param sch
    *   the other schema
    */
  def addAll(sch: Schema): Unit = {
    sch
      .getFields()
      .forEach(fldname => {
        add(fldname, sch);
      })
  }

  /** Return a collection containing the name of each field in the schema.
    * @return
    *   the collection of the schema's field names
    */
  def getFields(): java.util.List[String] = {
    fields;
  }

  /** Return true if the specified field is in the schema
    * @param fldname
    *   the name of the field
    * @return
    *   true if the field is in the schema
    */
  def hasField(fldname: String): Boolean = {
    fields.contains(fldname);
  }

  /** Return the type of the specified field, using the constants in {@link
    * java.sql.Types}.
    * @param fldname
    *   the name of the field
    * @return
    *   the integer type of the field
    */
  def getType(fldname: String): Int = {
    info.get(fldname).fieldtype
  }

  /** Return the conceptual length of the specified field. If the field is not a
    * string field, then the return value is undefined.
    * @param fldname
    *   the name of the field
    * @return
    *   the conceptual length of the field
    */
  def length(fldname: String) = {
    info.get(fldname).length;
  }
}

class FieldInfo(val fieldtype: Int, val length: Int) {}
