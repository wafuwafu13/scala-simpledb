package simpledb.buffer;

import org.scalatest.funsuite.AnyFunSuite;
import simpledb.file._;
import simpledb.server.SimpleDB;

class BufferTest extends AnyFunSuite {
  val db = new SimpleDB("buffertest", 400, 3); // only 3 buffers
  val bm = db.bufferMgr();

  val buff1: Buffer = bm.pin(new BlockId("testfile", 1));
  val p: Page = buff1.getContents();
  val n: Int = p.getInt(80);
  p.setInt(80, n + 1);
  buff1.setModified(1, 0); // placeholder values
  println("The new value is " + (n + 1));
  bm.unpin(buff1);
  // One of these pins will flush buff1 to disk:
  var buff2: Buffer = bm.pin(new BlockId("testfile", 2));
  val buff3: Buffer = bm.pin(new BlockId("testfile", 3));
  val buff4: Buffer = bm.pin(new BlockId("testfile", 4));

  bm.unpin(buff2);
  buff2 = bm.pin(new BlockId("testfile", 1));
  val p2: Page = buff2.getContents();
  p2.setInt(80, 9999); // This modification
  buff2.setModified(1, 0); // won't get written to disk.
}
