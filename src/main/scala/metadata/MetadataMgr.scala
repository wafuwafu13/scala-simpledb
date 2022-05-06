package simpledb.metadata;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record._;

class MetadataMgr(val isnew: Boolean, val tx: Transaction) {

  private val tblmgr: TableMgr = new TableMgr(isnew, tx);
  private val viewmgr: ViewMgr = new ViewMgr(isnew, tblmgr, tx);
  private val statmgr: StatMgr = new StatMgr(tblmgr, tx);
  private val idxmgr: IndexMgr = new IndexMgr(isnew, tblmgr, statmgr, tx);

  def createTable(tblname: String, sch: Schema, tx: Transaction): Unit = {
    tblmgr.createTable(tblname, sch, tx);
  }

  def getLayout(tblname: String, tx: Transaction): Layout = {
    tblmgr.getLayout(tblname, tx);
  }

  def createView(viewname: String, viewdef: String, tx: Transaction): Unit = {
    viewmgr.createView(viewname, viewdef, tx);
  }

  def getViewDef(viewname: String, tx: Transaction): String = {
    viewmgr.getViewDef(viewname, tx);
  }

  def createIndex(
      idxname: String,
      tblname: String,
      fldname: String,
      tx: Transaction
  ): Unit = {
    idxmgr.createIndex(idxname, tblname, fldname, tx);
  }

  def getIndexInfo(tblname: String, tx: Transaction): Map[String, IndexInfo] = {
    idxmgr.getIndexInfo(tblname, tx);
  }

  def getStatInfo(
      tblname: String,
      layout: Layout,
      tx: Transaction
  ): StatInfo = {
    statmgr.getStatInfo(tblname, layout, tx);
  }
}
