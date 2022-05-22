package simpledb.metadata;

import simpledb.server.SimpleDB;
import java.sql.Types.INTEGER;
import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;

class MetadataMgrTest extends AnyFunSuite {
  test("MetadataMgr") {
    val db = new SimpleDB("metadatamgrtest", 400, 8);
    val tx: Transaction = db.newTx();
    val mdm: MetadataMgr = new MetadataMgr(true, tx);

    val sch: Schema = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);

    // Part 1: Table Metadata
    mdm.createTable("MyTable", sch, tx);
    val layout: Layout = mdm.getLayout("MyTable", tx);
    val size: Int = layout.slotSize();
    val sch2: Schema = layout.getSchema();
    println("MyTable has slot size " + size);
    println("Its fields are:");
    sch2
      .getFields()
      .forEach(fldname => {
        var fieldtype: String = "";
        if (sch2.getType(fldname) == INTEGER)
          fieldtype = "int";
        else {
          val strlen: Int = sch2.length(fldname);
          fieldtype = "varchar(" + strlen + ")";
        }
        println(fldname + ": " + fieldtype);
      })

    // Part 2: Statistics Metadata
    val ts: TableScan = new TableScan(tx, "MyTable", layout);
    for (i <- 0 until 50) {
      ts.insert();
      val n: Int = Math.round((Math.random() * 50).asInstanceOf[Float]);
      ts.setInt("A", n);
      ts.setString("B", "rec" + n);
    }
    val si: StatInfo = mdm.getStatInfo("MyTable", layout, tx);
    println("B(MyTable) = " + si.blocksAccessed());
    println("R(MyTable) = " + si.recordsOutput());
    println("V(MyTable,A) = " + si.distinctValues("A"));
    println("V(MyTable,B) = " + si.distinctValues("B"));

    // Part 3: View Metadata
    val viewdef: String = "select B from MyTable where A = 1";
    mdm.createView("viewA", viewdef, tx);
    val v: String = mdm.getViewDef("viewA", tx);
    println("View def = " + v);

    // Part 4: Index Metadata
    mdm.createIndex("indexA", "MyTable", "A", tx);
    mdm.createIndex("indexB", "MyTable", "B", tx);
    val idxmap: Map[String, IndexInfo] = mdm.getIndexInfo("MyTable", tx);

    var ii: IndexInfo = idxmap.get("A");
    // TODO: implement
    // println("B(indexA) = " + ii.blocksAccessed());
    println("R(indexA) = " + ii.recordsOutput());
    println("V(indexA,A) = " + ii.distinctValues("A"));
    println("V(indexA,B) = " + ii.distinctValues("B"));

    ii = idxmap.get("B");
    // println("B(indexB) = " + ii.blocksAccessed());
    println("R(indexB) = " + ii.recordsOutput());
    println("V(indexB,A) = " + ii.distinctValues("A"));
    println("V(indexB,B) = " + ii.distinctValues("B"));
    tx.commit();
  }
}
