package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.RID;

/** This interface contains methods to traverse an index.
  * @author
  *   Edward Sciore
  */
trait Index {

  /** Positions the index before the first record having the specified search
    * key.
    * @param searchkey
    *   the search key value.
    */
  def beforeFirst(searchkey: Constant): Unit;

  /** Moves the index to the next record having the search key specified in the
    * beforeFirst method. Returns false if there are no more such index records.
    * @return
    *   false if no other index records have the search key.
    */
  def next(): Boolean;

  /** Returns the dataRID value stored in the current index record.
    * @return
    *   the dataRID stored in the current index record.
    */
  def getDataRid(): RID;

  /** Inserts an index record having the specified dataval and dataRID values.
    * @param dataval
    *   the dataval in the new index record.
    * @param datarid
    *   the dataRID in the new index record.
    */
  def insert(dataval: Constant, datarid: RID): Unit;

  /** Deletes the index record having the specified dataval and dataRID values.
    * @param dataval
    *   the dataval of the deleted index record
    * @param datarid
    *   the dataRID of the deleted index record
    */
  def delete(dataval: Constant, datarid: RID): Unit;

  /** Closes the index.
    */
  def close(): Unit;
}
