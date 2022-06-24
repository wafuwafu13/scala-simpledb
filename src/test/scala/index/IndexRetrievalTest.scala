package simpledb.index;

import java.util.Map;

import simpledb.metadata._;
import simpledb.plan._;
import simpledb.query._;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import org.scalatest.funsuite.AnyFunSuite;

class IndexRetrievalTest extends AnyFunSuite {
  val db: SimpleDB = new SimpleDB("indexretrieval", null, null);
  val tx: Transaction = db.newTx();
  val mdm: MetadataMgr = db.mdMgr();

  // Open a scan on the data table.
  val studentplan: Plan = new TablePlan(tx, "student", mdm);
  val studentscan: UpdateScan = studentplan.open().asInstanceOf[UpdateScan];

  // Open the index on MajorId.
  val indexes: Map[String, IndexInfo] = mdm.getIndexInfo("student", tx);
  val ii: IndexInfo = indexes.get("majorid");
  val idx: Index = ii.open();

  // Retrieve all index records having a dataval of 20.
  idx.beforeFirst(new Constant(20));
  while (idx.next()) {
    // Use the datarid to go to the corresponding STUDENT record.
    val datarid: RID = idx.getDataRid();
    studentscan.moveToRid(datarid);
    println(studentscan.getString("sname"));
  }

  // Close the index and the data table.
  idx.close();
  studentscan.close();
  tx.commit();
}
