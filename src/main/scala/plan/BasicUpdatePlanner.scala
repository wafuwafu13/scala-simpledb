package simpledb.plan;

import java.util.Iterator;
import simpledb.tx.Transaction;
import simpledb.parse._;
import simpledb.query._;
import simpledb.metadata.MetadataMgr;

/** The basic planner for SQL update statements.
  * @author
  *   sciore
  */
class BasicUpdatePlanner(val mdm: MetadataMgr) extends UpdatePlanner {
  def executeDelete(data: DeleteData, tx: Transaction): Int = {
    var p: Plan = new TablePlan(tx, data.tableName(), mdm);
    p = new SelectPlan(p, data.getPred());
    val us: UpdateScan = p.open().asInstanceOf[UpdateScan];
    var count: Int = 0;
    while (us.next()) {
      us.delete();
      count += 1;
    }
    us.close();
    count;
  }

  def executeModify(data: ModifyData, tx: Transaction): Int = {
    var p: Plan = new TablePlan(tx, data.tableName(), mdm);
    p = new SelectPlan(p, data.getPred());
    val us: UpdateScan = p.open().asInstanceOf[UpdateScan];
    var count: Int = 0;
    while (us.next()) {
      val value: Constant = data.newValue().evaluate(us);
      us.setVal(data.targetField(), value);
      count += 1;
    }
    us.close();
    count;
  }

  def executeInsert(data: InsertData, tx: Transaction): Int = {
    val p: Plan = new TablePlan(tx, data.tableName(), mdm);
    val us: UpdateScan = p.open().asInstanceOf[UpdateScan];
    us.insert();
    val iter: Iterator[Constant] = data.getVals().iterator();
    data
      .fields()
      .forEach(fldname => {
        val value: Constant = iter.next();
        us.setVal(fldname, value);
      })
    us.close();
    1;
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
