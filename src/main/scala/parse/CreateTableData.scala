package simpledb.parse;

import simpledb.record.Schema;

/** Data for the SQL <i>create table</i> statement.
  * @author
  *   Edward Sciore
  */
class CreateTableData(val tblname: String, val sch: Schema) {

  /** Returns the name of the new table.
    * @return
    *   the name of the new table
    */
  def tableName(): String = {
    tblname;
  }

  /** Returns the schema of the new table.
    * @return
    *   the schema of the new table
    */
  def newSchema(): Schema = {
    sch;
  }
}
