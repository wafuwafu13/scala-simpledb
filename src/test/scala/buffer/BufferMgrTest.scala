package simpledb.buffer;

import org.scalatest.funsuite.AnyFunSuite;
import simpledb.file._;
import simpledb.log._;
import java.io.File;

class BufferMgrTest extends AnyFunSuite {
  val path: String = "./resources/buffermgrtest";
  val blockSize: Int = 400;
  val fm: FileMgr = new FileMgr(new File(path), blockSize);
  val logfilename: String = "simpledb.log";
  val lm = new LogMgr(fm, logfilename);
  val bufferSize: Int = 3;
  val bm = new BufferMgr(fm, lm, bufferSize);

  val buff: Array[Buffer] = new Array[Buffer](6);
  var b0 = new BlockId("testfile", 0)
  var b1 = new BlockId("testfile", 1)
  var b2 = new BlockId("testfile", 2)
  buff(0) = bm.pin(b0);
  buff(1) = bm.pin(b1);
  buff(2) = bm.pin(b2);
  bm.unpin(buff(1));
  buff(1) = null;
  buff(3) = bm.pin(b0); // block 0 pinned twice
  buff(4) = bm.pin(b1); // block 1 repinned

  assert(bm.available() == 0);

  try {
    System.out.println("Attempting to pin block 3...");
    buff(5) =
      bm.pin(new BlockId("testfile", 3)); // will not work; no buffers left
  } catch {
    case e: BufferAbortException =>
      println("Exception: No available buffers\n");
  }

  bm.unpin(buff(2));
  buff(2) = null;
  buff(5) = bm.pin(new BlockId("testfile", 3)); // now this works

  assert(buff(0).block().number() == 0)
  assert(buff(3).block().number() == 0)
  assert(buff(4).block().number() == 1)
  assert(buff(5).block().number() == 3)
}
