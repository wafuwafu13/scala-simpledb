package simpledb.query;

/** The scan class corresponding to the <i>project</i> relational algebra
  * operator. All methods except hasField delegate their work to the underlying
  * scan.
  * @author
  *   Edward Sciore
  */
/** Create a project scan having the specified underlying scan and field list.
  * @param s
  *   the underlying scan
  * @param fieldlist
  *   the list of field names
  */
class ProjectScan(val s: Scan, val fieldlist: java.util.List[String])
    extends Scan {
  def beforeFirst(): Unit = {
    s.beforeFirst();
  }

  def next(): Boolean = {
    s.next();
  }

  def getInt(fldname: String): Int = {
    if (hasField(fldname))
      s.getInt(fldname);
    else
      throw new RuntimeException("field " + fldname + " not found.");
  }

  def getString(fldname: String): String = {
    if (hasField(fldname))
      s.getString(fldname);
    else
      throw new RuntimeException("field " + fldname + " not found.");
  }

  def getVal(fldname: String): Constant = {
    if (hasField(fldname))
      s.getVal(fldname);
    else
      throw new RuntimeException("field " + fldname + " not found.");
  }

  def hasField(fldname: String): Boolean = {
    fieldlist.contains(fldname);
  }

  def close(): Unit = {
    s.close();
  }
}
