package simpledb.metadata;

import simpledb.record._;
import simpledb.tx.Transaction;

class ViewMgr(val isNew: Boolean, val tblMgr: TableMgr, val tx: Transaction) {
  // the max chars in a view definition.
  private val MAX_VIEWDEF: Int = 100;

  if (isNew) {
    val sch: Schema = new Schema();
    sch.addStringField("viewname", 16); // TableMgr.MAX_NAME
    sch.addStringField("viewdef", MAX_VIEWDEF);
    tblMgr.createTable("viewcat", sch, tx);
  }

  def createView(vname: String, vdef: String, tx: Transaction): Unit = {
    val layout: Layout = tblMgr.getLayout("viewcat", tx);
    val ts: TableScan = new TableScan(tx, "viewcat", layout);
    ts.insert();
    ts.setString("viewname", vname);
    ts.setString("viewdef", vdef);
    ts.close();
  }

  def getViewDef(vname: String, tx: Transaction): String = {
    var result = "";
    val layout: Layout = tblMgr.getLayout("viewcat", tx);
    val ts: TableScan = new TableScan(tx, "viewcat", layout);
    var flag: Boolean = true;
    while (ts.next() && flag)
      if (ts.getString("viewname").equals(vname)) {
        result = ts.getString("viewdef");
        flag = false;
      }
    ts.close();
    return result;
  }
}
