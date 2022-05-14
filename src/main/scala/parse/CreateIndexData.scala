package simpledb.parse;

/** The parser for the <i>create index</i> statement.
  * @author
  *   Edward Sciore
  */
class CreateIndexData(
    val idxname: String,
    val tblname: String,
    val fldname: String
) {

  /** Returns the name of the index.
    * @return
    *   the name of the index
    */
  def indexName(): String = {
    idxname;
  }

  /** Returns the name of the indexed table.
    * @return
    *   the name of the indexed table
    */
  def tableName(): String = {
    tblname;
  }

  /** Returns the name of the indexed field.
    * @return
    *   the name of the indexed field
    */
  def fieldName(): String = {
    fldname;
  }
}
