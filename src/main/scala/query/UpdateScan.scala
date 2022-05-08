package simpledb.query;

import simpledb.record.RID;

/** The interface implemented by all updateable scans.
  * @author
  *   Edward Sciore
  */
trait UpdateScan extends Scan {

  /** Modify the field value of the current record.
    * @param fldname
    *   the name of the field
    * @param val
    *   the new value, expressed as a Constant
    */
  def setVal(fldname: String, value: Constant): Unit;

  /** Modify the field value of the current record.
    * @param fldname
    *   the name of the field
    * @param val
    *   the new integer value
    */
  def setInt(fldname: String, value: Int): Unit;

  /** Modify the field value of the current record.
    * @param fldname
    *   the name of the field
    * @param val
    *   the new string value
    */
  def setString(fldname: String, value: String): Unit;

  /** Insert a new record somewhere in the scan.
    */
  def insert(): Unit;

  /** Delete the current record from the scan.
    */
  def delete(): Unit;

  /** Return the id of the current record.
    * @return
    *   the id of the current record
    */
  def getRid(): RID;

  /** Position the scan so that the current record has the specified id.
    * @param rid
    *   the id of the desired record
    */
  def moveToRid(rid: RID): Unit;
}
