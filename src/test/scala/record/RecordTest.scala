package simpledb.record;

import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class RecordTest extends AnyFunSuite {
  test("Record") {
    val path: String = "./resources/recordtest";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);

    val sch: Schema = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);
    val layout: Layout = new Layout(sch, null, null);
    layout
      .getSchema()
      .getFields()
      .forEach(fldname => {
        val offset: Int = layout.offset(fldname);
        System.out.println(fldname + " has offset " + offset);
      })

    val blk: BlockId = tx.append("testfile");
    tx.pin(blk);
    val rp: RecordPage = new RecordPage(tx, blk, layout);
    rp.format();

    println("Filling the page with random records.");
    var slot: Int = rp.insertAfter(-1);
    while (slot >= 0) {
      val n: Int = Math.round((Math.random() * 50).asInstanceOf[Float]);
      rp.setInt(slot, "A", n);
      rp.setString(slot, "B", "rec" + n);
      println(
        "inserting into slot " + slot + ": {" + n + ", " + "rec" + n + "}"
      );
      slot = rp.insertAfter(slot);
    }

    println("Deleting these records, whose A-values are less than 25.");
    var count: Int = 0;
    slot = rp.nextAfter(-1);
    while (slot >= 0) {
      val a: Int = rp.getInt(slot, "A");
      val b: String = rp.getString(slot, "B");
      if (a < 25) {
        count += 1;
        println("slot " + slot + ": {" + a + ", " + b + "}");
        rp.delete(slot);
      }
      slot = rp.nextAfter(slot);
    }
    println(count + " values under 25 were deleted.\n");

    println("Here are the remaining records.");
    slot = rp.nextAfter(-1);
    while (slot >= 0) {
      val a: Int = rp.getInt(slot, "A");
      val b: String = rp.getString(slot, "B");
      println("slot " + slot + ": {" + a + ", " + b + "}");
      slot = rp.nextAfter(slot);
    }
    tx.unpin(blk);
    tx.commit();
  }
}
