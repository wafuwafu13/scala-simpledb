package simpledb.index.planner;

import java.util._;
import simpledb.tx.Transaction;
import simpledb.record._;
import simpledb.metadata._;
import simpledb.query._;
import simpledb.parse._;
import simpledb.plan._;
import simpledb.index.Index;

/** A modification of the basic update planner. It dispatches each update
  * statement to the corresponding index planner.
  * @author
  *   Edward Sciore
  */
class IndexUpdatePlanner(val mdm: MetadataMgr) extends UpdatePlanner {
  def executeInsert(data: InsertData, tx: Transaction): Int = {
    val tblname: String = data.tableName();
    val p: Plan = new TablePlan(tx, tblname, mdm);

    // first, insert the record
    val s: UpdateScan = p.open().asInstanceOf[UpdateScan];
    s.insert();
    val rid: RID = s.getRid();

    // then modify each field, inserting an index record if appropriate
    val indexes: Map[String, IndexInfo] = mdm.getIndexInfo(tblname, tx);
    val valIter: Iterator[Constant] = data.getVals().iterator();
    data
      .fields()
      .forEach(fldname => {
        val value: Constant = valIter.next();
        s.setVal(fldname, value);

        val ii: IndexInfo = indexes.get(fldname);
        if (ii != null) {
          val idx: Index = ii.open();
          idx.insert(value, rid);
          idx.close();
        }
      })

    s.close();
    1;
  }

  def executeDelete(data: DeleteData, tx: Transaction): Int = {
    val tblname: String = data.tableName();
    var p: Plan = new TablePlan(tx, tblname, mdm);
    p = new SelectPlan(p, data.getPred());
    val indexes: Map[String, IndexInfo] = mdm.getIndexInfo(tblname, tx);

    val s: UpdateScan = p.open().asInstanceOf[UpdateScan];
    var count: Int = 0;
    while (s.next()) {
      // first, delete the record's RID from every index
      val rid: RID = s.getRid();
      indexes
        .keySet()
        .forEach(fldname => {
          val value: Constant = s.getVal(fldname);
          val idx: Index = indexes.get(fldname).open();
          idx.delete(value, rid);
          idx.close();

        })
      // then delete the record
      s.delete();
      count += 1;
    }
    s.close();
    count;
  }

  def executeModify(data: ModifyData, tx: Transaction): Int = {
    val tblname: String = data.tableName();
    val fldname: String = data.targetField();
    var p: Plan = new TablePlan(tx, tblname, mdm);
    p = new SelectPlan(p, data.getPred());

    val ii: IndexInfo = mdm.getIndexInfo(tblname, tx).get(fldname);
    val idx: Index = if (ii == null) null else ii.open();

    val s: UpdateScan = p.open().asInstanceOf[UpdateScan];
    var count: Int = 0;
    while (s.next()) {
      // first, update the record
      val newval: Constant = data.newValue().evaluate(s);
      val oldval: Constant = s.getVal(fldname);
      s.setVal(data.targetField(), newval);

      // then update the appropriate index, if it exists
      if (idx != null) {
        val rid: RID = s.getRid();
        idx.delete(oldval, rid);
        idx.insert(newval, rid);
      }
      count += 1;
    }
    if (idx != null) idx.close();
    s.close();
    count;
  }

  def executeCreateTable(data: CreateTableData, tx: Transaction): Int = {
    mdm.createTable(data.tableName(), data.newSchema(), tx);
    0;
  }

  def executeCreateView(data: CreateViewData, tx: Transaction): Int = {
    mdm.createView(data.viewName(), data.viewDef(), tx);
    0;
  }

  def executeCreateIndex(data: CreateIndexData, tx: Transaction): Int = {
    mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
    0;
  }
}
