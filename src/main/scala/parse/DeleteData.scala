package simpledb.parse;

import simpledb.query._;

/** Data for the SQL <i>delete</i> statement.
  * @author
  *   Edward Sciore
  */
class DeleteData(val tblname: String, val pred: Predicate) {

  /** Returns the name of the affected table.
    * @return
    *   the name of the affected table
    */
  def tableName(): String = {
    tblname;
  }

  /** Returns the predicate that describes which records should be deleted.
    * @return
    *   the deletion predicate
    */
  def getPred(): Predicate = {
    pred;
  }
}
