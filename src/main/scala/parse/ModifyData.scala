package simpledb.parse;

import simpledb.query._;

/** Data for the SQL <i>update</i> statement.
  * @author
  *   Edward Sciore
  */
class ModifyData(
    val tblname: String,
    val fldname: String,
    val newval: Expression,
    val pred: Predicate
) {

  /** Returns the name of the affected table.
    * @return
    *   the name of the affected table
    */
  def tableName(): String = {
    tblname;
  }

  /** Returns the field whose values will be modified
    * @return
    *   the name of the target field
    */
  def targetField(): String = {
    fldname;
  }

  /** Returns an expression. Evaluating this expression for a record produces
    * the value that will be stored in the record's target field.
    * @return
    *   the target expression
    */
  def newValue(): Expression = {
    newval;
  }

  /** Returns the predicate that describes which records should be modified.
    * @return
    *   the modification predicate
    */
  def getPred(): Predicate = {
    pred;
  }
}
