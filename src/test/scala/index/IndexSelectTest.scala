package simpledb.index.query;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query._;
import simpledb.metadata._;
import simpledb.plan._;
import simpledb.index._;
import simpledb.index.planner.IndexSelectPlan;

import java.util.Map;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;

// Find the grades of student 6.

class IndexSelectTest extends AnyFunSuite {

  val db: SimpleDB = new SimpleDB("indexselect", null, null);
  val mdm: MetadataMgr = db.mdMgr();
  val tx: Transaction = db.newTx();

  // Find the index on StudentId.
  val indexes: Map[String, IndexInfo] = mdm.getIndexInfo("enroll", tx);
  val sidIdx: IndexInfo = indexes.get("studentid");

  // Get the plan for the Enroll table
  val enrollplan: Plan = new TablePlan(tx, "enroll", mdm);

  // // Create the selection constant
  // val c: Constant = new Constant(6);

  // // Two different ways to use the index in simpledb:
  // useIndexManually(sidIdx, enrollplan, c);
  // useIndexScan(sidIdx, enrollplan, c);

  // tx.commit()

  def useIndexManually(ii: IndexInfo, p: Plan, c: Constant): Unit = {
    // Open a scan on the table.
    val s: TableScan = p.open().asInstanceOf[TableScan]; // must be a table scan
    val idx: Index = ii.open();

    // Retrieve all index records having the specified dataval.
    idx.beforeFirst(c);
    while (idx.next()) {
      // Use the datarid to go to the corresponding Enroll record.
      val datarid: RID = idx.getDataRid();
      s.moveToRid(datarid); // table scans can move to a specified RID.
      println(s.getString("grade"));
    }
    idx.close();
    s.close();
  }

  def useIndexScan(ii: IndexInfo, p: Plan, c: Constant): Unit = {
    // Open an index select scan on the enroll table.
    val idxplan: Plan = new IndexSelectPlan(p, ii, c);
    val s: Scan = idxplan.open();

    while (s.next()) {
      println(s.getString("grade"));
    }
    s.close();
  }
}
