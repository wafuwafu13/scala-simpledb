package simpledb.index.query;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query._;
import simpledb.metadata._;
import simpledb.plan._;
import simpledb.index._;
import simpledb.index.planner.IndexJoinPlan;

import java.util.Map;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;

// Find the grades of student 6.

class IndexJoinTest extends AnyFunSuite {

  val db: SimpleDB = new SimpleDB("indexjoin", null, null);
  val mdm: MetadataMgr = db.mdMgr();
  val tx: Transaction = db.newTx();

  // Find the index on StudentId.
  val indexes: Map[String, IndexInfo] = mdm.getIndexInfo("enroll", tx);
  val sidIdx: IndexInfo = indexes.get("studentid");

  // Get the plan for the Enroll table
  val studentplan: Plan = new TablePlan(tx, "student", mdm);
  val enrollplan: Plan = new TablePlan(tx, "enroll", mdm);

  // Two different ways to use the index in simpledb:
  useIndexManually(studentplan, enrollplan, sidIdx, "sid");
  useIndexScan(studentplan, enrollplan, sidIdx, "sid");

  tx.commit()

  def useIndexManually(
      p1: Plan,
      p2: Plan,
      ii: IndexInfo,
      joinfield: String
  ): Unit = {
    // Open scans on the tables.
    val s1: Scan = p1.open();
    val s2: TableScan =
      p2.open().asInstanceOf[TableScan]; // must be a table scan

    val idx: Index = ii.open();

    // Loop through s1 records. For each value of the join field,
    // use the index to find the matching s2 records.
    while (s1.next()) {
      val c: Constant = s1.getVal(joinfield);
      idx.beforeFirst(c);
      while (idx.next()) {
        // Use each datarid to go to the corresponding Enroll record.
        val datarid: RID = idx.getDataRid();
        s2.moveToRid(datarid); // table scans can move to a specified RID.
        println(s2.getString("grade"));
      }
    }
    idx.close();
    s1.close();
    s2.close();
  }

  def useIndexScan(
      p1: Plan,
      p2: Plan,
      ii: IndexInfo,
      joinfield: String
  ): Unit = {
    // Open an index select scan on the enroll table.
    val idxplan: Plan = new IndexJoinPlan(p1, p2, ii, joinfield);
    val s: Scan = idxplan.open();

    while (s.next()) {
      println(s.getString("grade"));
    }
    s.close();
  }
}
