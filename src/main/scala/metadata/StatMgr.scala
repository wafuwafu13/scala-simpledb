package simpledb.metadata;

import java.util._;
import simpledb.tx.Transaction;
import simpledb.record._;
import collection.JavaConverters._;

/** The statistics manager is responsible for keeping statistical information
  * about each table. The manager does not store this information in the
  * database. Instead, it calculates this information on system startup, and
  * periodically refreshes it.
  * @author
  *   Edward Sciore
  */
class StatMgr(val tblMgr: TableMgr, val tx: Transaction) {
  private var tablestats: Map[String, StatInfo] =
    scala.collection.mutable.HashMap().asJava

  private var numcalls: Int = 0;

  /** Create the statistics manager. The initial statistics are calculated by
    * traversing the entire database.
    * @param tx
    *   the startup transaction
    */

  refreshStatistics(tx);

  /** Return the statistical information about the specified table.
    * @param tblname
    *   the name of the table
    * @param layout
    *   the table's layout
    * @param tx
    *   the calling transaction
    * @return
    *   the statistical information about the table
    */
  def getStatInfo(tblname: String, layout: Layout, tx: Transaction): StatInfo =
    synchronized {
      numcalls += 1;
      if (numcalls > 100)
        refreshStatistics(tx);
      var si: StatInfo = tablestats.get(tblname);
      if (si == null) {
        si = calcTableStats(tblname, layout, tx);
        tablestats.put(tblname, si);
      }
      si;
    }

  private def refreshStatistics(tx: Transaction): Unit = synchronized {
    tablestats = scala.collection.mutable.HashMap().asJava;
    numcalls = 0;
    val tcatlayout: Layout = tblMgr.getLayout("tblcat", tx);
    val tcat: TableScan = new TableScan(tx, "tblcat", tcatlayout);
    while (tcat.next()) {
      val tblname: String = tcat.getString("tblname");
      val layout: Layout = tblMgr.getLayout(tblname, tx);
      val si: StatInfo = calcTableStats(tblname, layout, tx);
      tablestats.put(tblname, si);
    }
    tcat.close();
  }

  private def calcTableStats(
      tblname: String,
      layout: Layout,
      tx: Transaction
  ): StatInfo = synchronized {
    var numRecs: Int = 0;
    var numblocks: Int = 0;
    val ts: TableScan = new TableScan(tx, tblname, layout);
    while (ts.next()) {
      numRecs += 1;
      numblocks = ts.getRid().blockNumber() + 1;
    }
    ts.close();
    new StatInfo(numblocks, numRecs);
  }
}
