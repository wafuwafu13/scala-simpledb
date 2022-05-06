package simpledb.metadata;

/** A StatInfo object holds three pieces of statistical information about a
  * table: the number of blocks, the number of records, and the number of
  * distinct values for each field.
  * @author
  *   Edward Sciore
  */
/** Create a StatInfo object. Note that the number of distinct values is not
  * passed into the constructor. The object fakes this value.
  * @param numblocks
  *   the number of blocks in the table
  * @param numrecs
  *   the number of records in the table
  */
class StatInfo(val numBlocks: Int, val numRecs: Int) {

  /** Return the estimated number of blocks in the table.
    * @return
    *   the estimated number of blocks in the table
    */
  def blocksAccessed(): Int = {
    numBlocks;
  }

  /** Return the estimated number of records in the table.
    * @return
    *   the estimated number of records in the table
    */
  def recordsOutput(): Int = {
    numRecs;
  }

  /** Return the estimated number of distinct values for the specified field.
    * This estimate is a complete guess, because doing something reasonable is
    * beyond the scope of this system.
    * @param fldname
    *   the name of the field
    * @return
    *   a guess as to the number of distinct field values
    */
  def distinctValues(fldname: String): Int = {
    1 + (numRecs / 3);
  }
}
