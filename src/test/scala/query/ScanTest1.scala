package simpledb.query;

import java.util._;
import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class ScanTest1 extends AnyFunSuite {
  test("Scan1") {
    val path: String = "./resources/scantest1";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);

    val sch1: Schema = new Schema();
    sch1.addIntField("A");
    sch1.addStringField("B", 9);
    val layout: Layout = new Layout(sch1, null, null);
    val s1: UpdateScan = new TableScan(tx, "T", layout);

    s1.beforeFirst();
    val n: Int = 200;
    println("Inserting " + n + " random records.");
    for (i <- 0 until n) {
      s1.insert();
      val k: Int = Math.round((Math.random() * 50).asInstanceOf[Float]);
      s1.setInt("A", k);
      s1.setString("B", "rec" + k);
    }
    s1.close();

    val s2: Scan = new TableScan(tx, "T", layout);
    // selecting all records where A=10
    val c: Constant = new Constant(10);
    val t: Term = new Term(new Expression("A"), new Expression(c));
    val pred: Predicate = new Predicate(t);
    println("The predicate is " + pred);
    val s3: Scan = new SelectScan(s2, pred);
    val fields: List[String] = new ArrayList[String]();
    fields.add("B");
    val s4: Scan = new ProjectScan(s3, fields);
    while (s4.next()) {
      println(s4.getString("B"));
    }
    s4.close();
    tx.commit();
  }
}
