package simpledb.metadata;

import java.util._;
import simpledb.tx.Transaction;
import simpledb.record.TableScan;
import simpledb.record._;
import collection.JavaConverters._;

/** The index manager. The index manager has similar functionality to the table
  * manager.
  * @author
  *   Edward Sciore
  */
class IndexMgr(
    val isnew: Boolean,
    val tblmgr: TableMgr,
    val statmgr: StatMgr,
    val tx: Transaction
) {

  /** Create the index manager. This constructor is called during system
    * startup. If the database is new, then the <i>idxcat</i> table is created.
    * @param isnew
    *   indicates whether this is a new database
    * @param tx
    *   the system startup transaction
    */

  if (isnew) {
    val sch: Schema = new Schema();
    sch.addStringField("indexname", 16); // TableMgr.MAX_NAME
    sch.addStringField("tablename", 16);
    sch.addStringField("fieldname", 16);
    tblmgr.createTable("idxcat", sch, tx);
  }

  private val layout: Layout = tblmgr.getLayout("idxcat", tx);

  /** Create an index of the specified type for the specified field. A unique ID
    * is assigned to this index, and its information is stored in the idxcat
    * table.
    * @param idxname
    *   the name of the index
    * @param tblname
    *   the name of the indexed table
    * @param fldname
    *   the name of the indexed field
    * @param tx
    *   the calling transaction
    */
  def createIndex(
      idxname: String,
      tblname: String,
      fldname: String,
      tx: Transaction
  ): Unit = {
    val ts: TableScan = new TableScan(tx, "idxcat", layout);
    ts.insert();
    ts.setString("indexname", idxname);
    ts.setString("tablename", tblname);
    ts.setString("fieldname", fldname);
    ts.close();
  }

  /** Return a map containing the index info for all indexes on the specified
    * table.
    * @param tblname
    *   the name of the table
    * @param tx
    *   the calling transaction
    * @return
    *   a map of IndexInfo objects, keyed by their field names
    */
  def getIndexInfo(tblname: String, tx: Transaction): Map[String, IndexInfo] = {
    val result: Map[String, IndexInfo] =
      scala.collection.mutable.HashMap().asJava
    val ts: TableScan = new TableScan(tx, "idxcat", layout);
    while (ts.next())
      if (ts.getString("tablename").equals(tblname)) {
        val idxname: String = ts.getString("indexname");
        val fldname: String = ts.getString("fieldname");
        val tblLayout: Layout = tblmgr.getLayout(tblname, tx);
        val tblsi: StatInfo = statmgr.getStatInfo(tblname, tblLayout, tx);
        val ii: IndexInfo =
          new IndexInfo(idxname, fldname, tblLayout.getSchema(), tx, tblsi);
        result.put(fldname, ii);
      }
    ts.close();
    result;
  }
}
