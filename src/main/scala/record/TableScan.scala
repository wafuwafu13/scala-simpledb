package simpledb.record;

import java.sql.Types.INTEGER;
import simpledb.file.BlockId;
import simpledb.query._;
import simpledb.tx.Transaction;

/** Provides the abstraction of an arbitrarily large array of records.
  * @author
  *   sciore
  */
class TableScan(val tx: Transaction, val tblname: String, val layout: Layout)
    extends UpdateScan {

  private var rp: RecordPage = null;
  private var currentslot: Int = 0;

  private val filename: String = tblname + ".tbl";
  if (tx.size(filename) == 0) {
    moveToNewBlock();
  } else {
    moveToBlock(0);
  }

  // Methods that implement Scan

  def beforeFirst(): Unit = {
    moveToBlock(0);
  }

  def next(): Boolean = {
    currentslot = rp.nextAfter(currentslot);
    while (currentslot < 0) {
      if (atLastBlock()) { return false; }

      moveToBlock(rp.block().number() + 1);
      currentslot = rp.nextAfter(currentslot);
    }
    true;
  }

  def getInt(fldname: String): Int = {
    rp.getInt(currentslot, fldname);
  }

  def getString(fldname: String): String = {
    rp.getString(currentslot, fldname);
  }

  def getVal(fldname: String): Constant = {
    if (layout.getSchema().getType(fldname) == INTEGER) {
      new Constant(getInt(fldname));
    } else { new Constant(getString(fldname)); }

  }

  def hasField(fldname: String): Boolean = {
    layout.getSchema().hasField(fldname);
  }

  def close(): Unit = {
    if (rp != null) { tx.unpin(rp.block()); }

  }

  // Methods that implement UpdateScan

  def setInt(fldname: String, value: Int): Unit = {
    rp.setInt(currentslot, fldname, value);
  }

  def setString(fldname: String, value: String): Unit = {
    rp.setString(currentslot, fldname, value);
  }

  def setVal(fldname: String, value: Constant): Unit = {
    if (layout.getSchema().getType(fldname) == INTEGER)
      setInt(fldname, value.asInt());
    else
      setString(fldname, value.asString());
  }

  def insert(): Unit = {
    currentslot = rp.insertAfter(currentslot);
    while (currentslot < 0) {
      if (atLastBlock())
        moveToNewBlock();
      else
        moveToBlock(rp.block().number() + 1);
      currentslot = rp.insertAfter(currentslot);
    }
  }

  def delete(): Unit = {
    rp.delete(currentslot);
  }

  def moveToRid(rid: RID): Unit = {
    close();
    val blk = new BlockId(filename, rid.blockNumber());
    rp = new RecordPage(tx, blk, layout);
    currentslot = rid.getSlot();
  }

  def getRid(): RID = {
    new RID(rp.block().number(), currentslot);
  }

  // Private auxiliary methods

  private def moveToBlock(blknum: Int): Unit = {
    close();
    val blk = new BlockId(filename, blknum);
    rp = new RecordPage(tx, blk, layout);
    currentslot = -1;
  }

  private def moveToNewBlock(): Unit = {
    close();
    val blk = tx.append(filename);
    rp = new RecordPage(tx, blk, layout);
    rp.format();
    currentslot = -1;
  }

  private def atLastBlock(): Boolean = {
    rp.block().number() == tx.size(filename) - 1;
  }
}
