package simpledb.parse;

/** Data for the SQL <i>create view</i> statement.
  * @author
  *   Edward Sciore
  */
class CreateViewData(val viewname: String, val qrydata: QueryData) {

  /** Returns the name of the new view.
    * @return
    *   the name of the new view
    */
  def viewName(): String = {
    viewname;
  }

  /** Returns the definition of the new view.
    * @return
    *   the definition of the new view
    */
  def viewDef(): String = {
    qrydata.toString();
  }
}
