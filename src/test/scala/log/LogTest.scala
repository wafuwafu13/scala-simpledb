package simpledb.log;

import java.util.Iterator;
import simpledb.file.Page;
import simpledb.file.FileMgr;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;
import java.nio.charset._;

class LogTest extends AnyFunSuite {
  private def printLogRecords(lm: LogMgr, msg: String): Unit = {
    println(msg);
    val iter: Iterator[Array[Byte]] = lm.iterator();
    while (iter.hasNext()) {
      val rec: Array[Byte] = iter.next();
      val p: Page = new Page(rec);
      val s: String = p.getString(0);
      val npos: Int = p.maxLength(s.length());
      val value: Int = p.getInt(npos);
      println("[" + s + ", " + value + "]");
    }
    println();
  }

  private def createRecords(lm: LogMgr, start: Int, end: Int): Unit = {
    print("Creating records: ");
    for (i <- start to end) {
      val rec: Array[Byte] = createLogRecord("record" + i, i + 100);
      val lsn: Int = lm.append(rec);
      print(lsn + " ");
    }
    println();
  }

  // Create a log record having two values: a string and an integer.
  private def createLogRecord(s: String, n: Int): Array[Byte] = {
    val spos: Int = 0;
    val npos: Int = spos + maxLength(s.length());
    val b: Array[Byte] = new Array[Byte](npos + Integer.BYTES);

    val p: Page = new Page(b);
    p.setString(spos, s);
    p.setInt(npos, n);
    b;
  }

  def maxLength(strlen: Int): Int = {
    val bytesPerChar: Float =
      StandardCharsets.US_ASCII.newEncoder().maxBytesPerChar();
    Integer.BYTES + (strlen * bytesPerChar.asInstanceOf[Int]);
  }

  test("Log") {
    val path: String = "./resources/logtest";
    val blockSize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), 400);
    val logfilename: String = "simpledb.log";
    val lm = new LogMgr(fm, logfilename);

    printLogRecords(
      lm,
      "The initial empty log file:"
    ); // print an empty log file
    println("done");
    createRecords(lm, 1, 35);
    assert(20 == lm.getLastSavedLSN());
    printLogRecords(lm, "The log file now has these records:");
    createRecords(lm, 36, 70);
    lm.flush(65);
    println(70 == lm.getLastSavedLSN());
    printLogRecords(lm, "The log file now has these records:");
  }
}
