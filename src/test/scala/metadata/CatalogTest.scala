package simpledb.metadata;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;

class CatalogTest extends AnyFunSuite {
  test("Catalog") {
    val db = new SimpleDB("tabletest", 400, 8);
    val tx: Transaction = db.newTx();
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
