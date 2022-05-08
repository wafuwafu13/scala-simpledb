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

class ScanTest2 extends AnyFunSuite {
  test("Scan2") {
    val path: String = "./resources/scantest2";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);

    val sch1: Schema = new Schema();
    sch1.addIntField("A");
    sch1.addStringField("B", 9);
    val layout1: Layout = new Layout(sch1, null, null);
    val us1: UpdateScan = new TableScan(tx, "T1", layout1);
    us1.beforeFirst();
    val n: Int = 200;
    println("Inserting " + n + " records into T1.");
    for (i <- 0 until n) {
      us1.insert();
      us1.setInt("A", i);
      us1.setString("B", "bbb" + i);
    }
    us1.close();

    val sch2: Schema = new Schema();
    sch2.addIntField("C");
    sch2.addStringField("D", 9);
    val layout2: Layout = new Layout(sch2, null, null);
    val us2: UpdateScan = new TableScan(tx, "T2", layout2);
    us2.beforeFirst();
    System.out.println("Inserting " + n + " records into T2.");
    for (i <- 0 until n) {
      us2.insert();
      us2.setInt("C", n - i - 1);
      us2.setString("D", "ddd" + (n - i - 1));
    }
    us2.close();

    val s1: Scan = new TableScan(tx, "T1", layout1);
    val s2: Scan = new TableScan(tx, "T2", layout2);
    val s3: Scan = new ProductScan(s1, s2);
    // selecting all records where A=C
    val t: Term = new Term(new Expression("A"), new Expression("C"));
    val pred: Predicate = new Predicate(t);
    println("The predicate is " + pred);
    val s4: Scan = new SelectScan(s3, pred);

    // projecting on [B,D]
    val c: List[String] = Arrays.asList("B", "D");
    val s5: Scan = new ProjectScan(s4, c);
    while (s5.next())
      println(s5.getString("B") + " " + s5.getString("D"));
    s5.close();
    tx.commit();
  }
}
