package simpledb.parse;

import java.util._;

import simpledb.query._;

/** Data for the SQL <i>select</i> statement.
  * @author
  *   Edward Sciore
  */
class QueryData(
    val fields: List[String],
    val tables: Collection[String],
    val pred: Predicate
) {

  /** Returns the fields mentioned in the select clause.
    * @return
    *   a list of field names
    */
  def getFields(): List[String] = {
    fields;
  }

  /** Returns the tables mentioned in the from clause.
    * @return
    *   a collection of table names
    */
  def getTables(): Collection[String] = {
    tables;
  }

  /** Returns the predicate that describes which records should be in the output
    * table.
    * @return
    *   the query predicate
    */
  def getPred(): Predicate = {
    pred;
  }

  override def toString(): String = {
    var result: String = "select ";
    fields.forEach(fldname => {
      result += fldname + ", ";
    })
    result = result.substring(0, result.length() - 2); // remove final comma
    result += " from ";
    tables.forEach(tblname => {
      result += tblname + ", ";
    })
    result = result.substring(0, result.length() - 2); // remove final comma
    val predstring: String = pred.toString();
    if (!predstring.equals(""))
      result += " where " + predstring;
    result;
  }
}
