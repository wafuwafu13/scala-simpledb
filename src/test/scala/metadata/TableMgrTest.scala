package simpledb.metadata;

import simpledb.server.SimpleDB;
import java.sql.Types.INTEGER;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;

class TableMgrTest extends AnyFunSuite {
  test("TableMgr") {
    val db = new SimpleDB("resources/tblmgrtest", 400, 8);
    val tx: Transaction = db.newTx();
    val tm: TableMgr = new TableMgr(true, tx);

    val sch: Schema = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);
    tm.createTable("MyTable", sch, tx);

    val layout: Layout = tm.getLayout("MyTable", tx);
    val size: Int = layout.slotSize();
    val sch2: Schema = layout.getSchema();
    println("MyTable has slot size " + size);
    println("Its fields are:");
    sch2
      .getFields()
      .forEach(fldname => {
        var schematype: String = "";
        if (sch2.getType(fldname) == INTEGER)
          schematype = "int";
        else {
          val strlen: Int = sch2.length(fldname);
          schematype = "varchar(" + strlen + ")";
        }
        println(fldname + ": " + schematype);
      })
    tx.commit();
  }
}
