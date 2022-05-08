package simpledb.query;

import simpledb.record._;

/** The scan class corresponding to the <i>select</i> relational algebra
  * operator. All methods except next delegate their work to the underlying
  * scan.
  * @author
  *   Edward Sciore
  */
/** Create a select scan having the specified underlying scan and predicate.
  * @param s
  *   the scan of the underlying query
  * @param pred
  *   the selection predicate
  */
class SelectScan(val s: Scan, val pred: Predicate) extends Scan {
  // Scan methods

  def beforeFirst(): Unit = {
    s.beforeFirst();
  }

  def next(): Boolean = {
    while (s.next()) {
      if (pred.isSatisfied(s))
        return true;
    }
    false;
  }

  def getInt(fldname: String): Int = {
    s.getInt(fldname);
  }

  def getString(fldname: String): String = {
    s.getString(fldname);
  }

  def getVal(fldname: String): Constant = {
    s.getVal(fldname);
  }

  def hasField(fldname: String): Boolean = {
    s.hasField(fldname);
  }

  def close(): Unit = {
    s.close();
  }

  // UpdateScan methods

  def setInt(fldname: String, value: Int): Unit = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.setInt(fldname, value);
  }

  def setString(fldname: String, value: String) = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.setString(fldname, value);
  }

  def setVal(fldname: String, value: Constant): Unit = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.setVal(fldname, value);
  }

  def delete(): Unit = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.delete();
  }

  def insert(): Unit = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.insert();
  }

  def getRid(): RID = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.getRid();
  }

  def moveToRid(rid: RID): Unit = {
    val us: UpdateScan = s.asInstanceOf[UpdateScan];
    us.moveToRid(rid);
  }
}
