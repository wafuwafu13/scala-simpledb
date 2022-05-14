package simpledb.parse;

import java.util._;
import simpledb.query.Constant;

/** Data for the SQL <i>insert</i> statement.
  * @author
  *   Edward Sciore
  */
class InsertData(
    val tblname: String,
    val flds: List[String],
    val vals: List[Constant]
) {

  /** Returns the name of the affected table.
    * @return
    *   the name of the affected table
    */
  def tableName(): String = {
    tblname;
  }

  /** Returns a list of fields for which values will be specified in the new
    * record.
    * @return
    *   a list of field names
    */
  def fields(): List[String] = {
    flds;
  }

  /** Returns a list of values for the specified fields. There is a one-one
    * correspondence between this list of values and the list of fields.
    * @return
    *   a list of Constant values.
    */
  def getVals(): List[Constant] = {
    vals;
  }
}
