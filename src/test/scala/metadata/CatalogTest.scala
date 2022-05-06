package simpledb.metadata;

import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class CatalogTest extends AnyFunSuite {
  test("Catalog") {
    val path: String = "./resources/tabletest";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);
    // TODO: false
    val tm: TableMgr = new TableMgr(true, tx);
    val tcatLayout: Layout = tm.getLayout("tblcat", tx);

    println("Here are all the tables and their lengths.");
    var ts: TableScan = new TableScan(tx, "tblcat", tcatLayout);
    while (ts.next()) {
      val tname: String = ts.getString("tblname");
      val slotsize: Int = ts.getInt("slotsize");
      println(tname + " " + slotsize);
    }
    ts.close();

    println("\nHere are the fields for each table and their offsets");
    val fcatLayout: Layout = tm.getLayout("fldcat", tx);
    ts = new TableScan(tx, "fldcat", fcatLayout);
    while (ts.next()) {
      val tname: String = ts.getString("tblname");
      val fname: String = ts.getString("fldname");
      val offset: Int = ts.getInt("offset");
      println(tname + " " + fname + " " + offset);
    }
    ts.close();
  }
}
