package simpledb.tx;

import simpledb.buffer.BufferMgr;
import simpledb.file._;
import simpledb.log.LogMgr;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class ConcurrencyTest extends AnyFunSuite {
  test("Log") {
    val path: String = "./resources/concurrencytest";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val a: A = new A(fm, lm, bm); new Thread(a).start();
    val b: B = new B(fm, lm, bm); new Thread(b).start();
    val c: C = new C(fm, lm, bm); new Thread(c).start();
  }
}

class A(val fm: FileMgr, lm: LogMgr, bm: BufferMgr) extends Runnable {
  def run(): Unit = {
    try {
      val txA: Transaction = new Transaction(fm, lm, bm);
      val blk1: BlockId = new BlockId("testfile", 1);
      val blk2: BlockId = new BlockId("testfile", 2);
      txA.pin(blk1);
      txA.pin(blk2);
      println("Tx A: request slock 1");
      txA.getInt(blk1, 0);
      println("Tx A: receive slock 1");
      // TODO: why have to remove
      // Thread.sleep(1000);
      println("Tx A: request slock 2");
      txA.getInt(blk2, 0);
      println("Tx A: receive slock 2");
      txA.commit();
      println("Tx A: commit");
    } catch {
      case e: InterruptedException => {}
    };
  }
}

class B(val fm: FileMgr, lm: LogMgr, bm: BufferMgr) extends Runnable {
  def run(): Unit = {
    try {
      val txB: Transaction = new Transaction(fm, lm, bm);
      val blk1: BlockId = new BlockId("testfile", 1);
      val blk2: BlockId = new BlockId("testfile", 2);
      txB.pin(blk1);
      txB.pin(blk2);
      println("Tx B: request xlock 2");
      txB.setInt(blk2, 0, 0, false);
      println("Tx B: receive xlock 2");
      // Thread.sleep(1000);
      println("Tx B: request slock 1");
      txB.getInt(blk1, 0);
      println("Tx B: receive slock 1");
      txB.commit();
      println("Tx B: commit");
    } catch {
      case e: InterruptedException => {}
    };
  }
}

class C(val fm: FileMgr, lm: LogMgr, bm: BufferMgr) extends Runnable {
  def run(): Unit = {
    try {
      val txC: Transaction = new Transaction(fm, lm, bm);
      val blk1: BlockId = new BlockId("testfile", 1);
      val blk2: BlockId = new BlockId("testfile", 2);
      txC.pin(blk1);
      txC.pin(blk2);
      // Thread.sleep(500);
      println("Tx C: request xlock 1");
      txC.setInt(blk1, 0, 0, false);
      println("Tx C: receive xlock 1");
      // Thread.sleep(1000);
      println("Tx C: request slock 2");
      txC.getInt(blk2, 0);
      println("Tx C: receive slock 2");
      txC.commit();
      println("Tx C: commit");
    } catch {
      case e: InterruptedException => {}
    };
  }
}
