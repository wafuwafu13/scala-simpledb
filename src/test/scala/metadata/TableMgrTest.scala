package simpledb.metadata;

import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import java.sql.Types.INTEGER;
import simpledb.tx.Transaction;
import simpledb.record._;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class TableMgrTest extends AnyFunSuite {
  test("TableMgr") {
    val path: String = "./resources/tblmgrtest";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);
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
