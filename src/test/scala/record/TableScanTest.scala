package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.file._;
import simpledb.tx.Transaction;
import org.scalatest.funsuite.AnyFunSuite;

class TableScanTest extends AnyFunSuite {
  test("TableScan") {
    val db = new SimpleDB("tabletest", null, null);
    val tx: Transaction = db.newTx();

    val sch = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);
    val layout = new Layout(sch, null, null);
    layout
      .getSchema()
      .getFields()
      .forEach(fldname => {
        val offset: Int = layout.offset(fldname);
        println(fldname + " has offset " + offset);
      })

    println("Filling the table with 50 random records.");
    val ts: TableScan = new TableScan(tx, "T", layout);
    for (i <- 0 until 50) {
      ts.insert();
      val n: Int = Math.round((Math.random() * 50).asInstanceOf[Float]);
      ts.setInt("A", n);
      ts.setString("B", "rec" + n);
      println(
        "inserting into slot " + ts
          .getRid() + ": {" + n + ", " + "rec" + n + "}"
      );
    }

    println("Deleting these records, whose A-values are less than 25.");
    var count = 0;
    ts.beforeFirst();
    while (ts.next()) {
      val a: Int = ts.getInt("A");
      val b: String = ts.getString("B");
      if (a < 25) {
        count += 1;
        println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
        ts.delete();
      }
    }
    println(count + " values under 25 were deleted.\n");

    println("Here are the remaining records.");
    ts.beforeFirst();
    while (ts.next()) {
      val a: Int = ts.getInt("A");
      val b: String = ts.getString("B");
      println("slot " + ts.getRid() + ": {" + a + ", " + b + "}");
    }
    ts.close();
    tx.commit();
  }
}
