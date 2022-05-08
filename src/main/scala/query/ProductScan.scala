package simpledb.query;

/** The scan class corresponding to the <i>product</i> relational algebra
  * operator.
  * @author
  *   Edward Sciore
  */
class ProductScan(val s1: Scan, val s2: Scan) extends Scan {

  /** Create a product scan having the two underlying scans.
    * @param s1
    *   the LHS scan
    * @param s2
    *   the RHS scan
    */
  beforeFirst();

  /** Position the scan before its first record. In particular, the LHS scan is
    * positioned at its first record, and the RHS scan is positioned before its
    * first record.
    * @see
    *   simpledb.query.Scan#beforeFirst()
    */
  def beforeFirst(): Unit = {
    s1.beforeFirst();
    s1.next();
    s2.beforeFirst();
  }

  /** Move the scan to the next record. The method moves to the next RHS record,
    * if possible. Otherwise, it moves to the next LHS record and the first RHS
    * record. If there are no more LHS records, the method returns false.
    * @see
    *   simpledb.query.Scan#next()
    */
  def next(): Boolean = {
    if (s2.next())
      true;
    else {
      s2.beforeFirst();
      s2.next() && s1.next();
    }
  }

  /** Return the integer value of the specified field. The value is obtained
    * from whichever scan contains the field.
    * @see
    *   simpledb.query.Scan#getInt(java.lang.String)
    */
  def getInt(fldname: String): Int = {
    if (s1.hasField(fldname))
      s1.getInt(fldname);
    else
      s2.getInt(fldname);
  }

  /** Returns the string value of the specified field. The value is obtained
    * from whichever scan contains the field.
    * @see
    *   simpledb.query.Scan#getString(java.lang.String)
    */
  def getString(fldname: String): String = {
    if (s1.hasField(fldname))
      s1.getString(fldname);
    else
      s2.getString(fldname);
  }

  /** Return the value of the specified field. The value is obtained from
    * whichever scan contains the field.
    * @see
    *   simpledb.query.Scan#getVal(java.lang.String)
    */
  def getVal(fldname: String): Constant = {
    if (s1.hasField(fldname))
      s1.getVal(fldname);
    else
      s2.getVal(fldname);
  }

  /** Returns true if the specified field is in either of the underlying scans.
    * @see
    *   simpledb.query.Scan#hasField(java.lang.String)
    */
  def hasField(fldname: String): Boolean = {
    s1.hasField(fldname) || s2.hasField(fldname);
  }

  /** Close both underlying scans.
    * @see
    *   simpledb.query.Scan#close()
    */
  def close(): Unit = {
    s1.close();
    s2.close();
  }
}
