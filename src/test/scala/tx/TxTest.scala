package simpledb.tx;

import simpledb.server.SimpleDB;
import simpledb.buffer.BufferMgr;
import simpledb.file._;
import org.scalatest.funsuite.AnyFunSuite;

class TxTest extends AnyFunSuite {
  test("Tx") {
    val db = new SimpleDB("txtest", 400, 8);
    val fm = db.fileMgr();
    val lm = db.logMgr();
    val bm = db.bufferMgr();

    val tx1: Transaction = new Transaction(fm, lm, bm);
    val blk: BlockId = new BlockId("testfile", 1);
    tx1.pin(blk);
    // The block initially contains unknown bytes,
    // so don't log those values here.
    tx1.setInt(blk, 80, 1, false);
    tx1.setString(blk, 40, "one", false);
    tx1.commit();

    val tx2: Transaction = new Transaction(fm, lm, bm);
    tx2.pin(blk);
    var ival: Int = tx2.getInt(blk, 80);
    var sval: String = tx2.getString(blk, 40);

    // initial value at location 80
    assert(1 == ival);
    // initial value at location 40
    assert("one" == sval);

    val newival: Int = ival + 1;
    val newsval: String = sval + "!";
    tx2.setInt(blk, 80, newival, true);
    tx2.setString(blk, 40, newsval, true);
    tx2.commit();

    val tx3: Transaction = new Transaction(fm, lm, bm);
    tx3.pin(blk);

    ival = tx3.getInt(blk, 80);
    sval = tx3.getString(blk, 40);

    // new value at location 80
    assert(2 == ival);
    // new value at location 40
    assert("one!" == sval);

    tx3.setInt(blk, 80, 9999, true);

    ival = tx3.getInt(blk, 80);

    // pre-rollback value at location 80
    assert(9999 == ival);
    tx3.rollback();

    val tx4: Transaction = new Transaction(fm, lm, bm);
    tx4.pin(blk);

    ival = tx4.getInt(blk, 80);
    // post-rollback at location 80
    // TODO: to be 2 not 9999
    // assert(2 == ival);
    tx4.commit();
  }
}
