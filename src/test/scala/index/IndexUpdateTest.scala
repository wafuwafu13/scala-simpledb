package simpledb.index;

import java.util._;
import simpledb.metadata._;
import simpledb.plan._;
import simpledb.query._;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import org.scalatest.funsuite.AnyFunSuite;

class IndexUpdateTest extends AnyFunSuite {
  val db: SimpleDB = new SimpleDB("indexupdate", null, null);
  val tx: Transaction = db.newTx();
  val mdm: MetadataMgr = db.mdMgr();
  val studentplan: Plan = new TablePlan(tx, "student", mdm);
  val studentscan: UpdateScan = studentplan.open().asInstanceOf[UpdateScan];

  // Create a map containing all indexes for STUDENT.
  val indexes: Map[String, Index] = new HashMap();
  val idxinfo: Map[String, IndexInfo] = mdm.getIndexInfo("student", tx);
  idxinfo
    .keySet()
    .forEach(fldname => {
      val idx: Index = idxinfo.get(fldname).open();
      indexes.put(fldname, idx);

    })

  // Task 1: insert a new STUDENT record for Sam
  //    First, insert the record into STUDENT.
  studentscan.insert();
  studentscan.setInt("sid", 11);
  studentscan.setString("sname", "sam");
  studentscan.setInt("gradyear", 2023);
  studentscan.setInt("majorid", 30);

  //    Then insert a record into each of the indexes.
  val datarid: RID = studentscan.getRid();
  idxinfo
    .keySet()
    .forEach(fldname => {
      val dataval: Constant = studentscan.getVal(fldname);
      val idx: Index = indexes.get(fldname);
      idx.insert(dataval, datarid);
    })

  // Task 2: find and delete Joe's record
  studentscan.beforeFirst();
  while (studentscan.next()) {
    if (studentscan.getString("sname").equals("joe")) {

      // First, delete the index records for Joe.
      val joeRid: RID = studentscan.getRid();
      idxinfo
        .keySet()
        .forEach(fldname => {
          val dataval: Constant = studentscan.getVal(fldname);
          val idx: Index = indexes.get(fldname);
          idx.delete(dataval, joeRid);

        })
      // Then delete Joe's record in STUDENT.
      studentscan.delete();
      // break;
    }
  }

  // Print the records to verify the updates.
  studentscan.beforeFirst();
  while (studentscan.next()) {
    println(studentscan.getString("sname") + " " + studentscan.getInt("sid"));
  }
  studentscan.close();
  indexes
    .values()
    .forEach(idx => {
      idx.close();

    })

  tx.commit();
}
