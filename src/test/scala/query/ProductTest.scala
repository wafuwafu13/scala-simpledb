package simpledb.query;

import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class ProductTest extends AnyFunSuite {
  test("Product") {
    val path: String = "./resources/producttest";
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
    val ts1: TableScan = new TableScan(tx, "T1", layout1);

    val sch2: Schema = new Schema();
    sch2.addIntField("C");
    sch2.addStringField("D", 9);
    val layout2: Layout = new Layout(sch2, null, null);
    val ts2: TableScan = new TableScan(tx, "T2", layout2);

    ts1.beforeFirst();
    var n: Int = 200;
    println("Inserting " + n + " records into T1.");
    for (i <- 0 until n) {
      ts1.insert();
      ts1.setInt("A", i);
      ts1.setString("B", "aaa" + i);
    }
    ts1.close();

    ts2.beforeFirst();
    println("Inserting " + n + " records into T2.");
    for (i <- 0 until n) {
      ts2.insert();
      ts2.setInt("C", n - i - 1);
      ts2.setString("D", "bbb" + (n - i - 1));
    }
    ts2.close();

    val s1: Scan = new TableScan(tx, "T1", layout1);
    val s2: Scan = new TableScan(tx, "T2", layout2);
    val s3: Scan = new ProductScan(s1, s2);
    while (s3.next())
      println(s3.getString("B"));
    s3.close();
    tx.commit();
  }
}
