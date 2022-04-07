package simpledb.log;

import java.util.Iterator;
import simpledb.file._;

/** The log manager, which is responsible for writing log records into a log
  * file. The tail of the log is kept in a bytebuffer, which is flushed to disk
  * when needed.
  * @author
  *   Edward Sciore
  */
class LogMgr(val fm: FileMgr, val logfile: String) {

  /** Creates the manager for the specified log file. If the log file does not
    * yet exist, it is created with an empty first block.
    * @param FileMgr
    *   the file manager
    * @param logfile
    *   the name of the log file
    */
  protected var latestLSN: Int = 0;
  var lastSavedLSN: Int = 0;
  val b: Array[Byte] = new Array[Byte](fm.blocksize);
  protected var logpage = new Page(b);
  val logsize: Int = fm.length(logfile);
  protected var currentblk: BlockId = new BlockId(logfile, logsize - 1);
  if (logsize == 0)
    currentblk = appendNewBlock();
  else {
    fm.read(currentblk, logpage);
  }

  /** Ensures that the log record corresponding to the specified LSN has been
    * written to disk. All earlier log records will also be written to disk.
    * @param lsn
    *   the LSN of a log record
    */
  def flush(lsn: Int): Unit = {
    if (lsn >= lastSavedLSN)
      flush();
  }

  def iterator() = {
    flush();
    new LogIterator(fm, currentblk);
  }

  /** Appends a log record to the log buffer. The record consists of an
    * arbitrary array of bytes. Log records are written right to left in the
    * buffer. The size of the record is written before the bytes. The beginning
    * of the buffer contains the location of the last-written record (the
    * "boundary"). Storing the records backwards makes it easy to read them in
    * reverse order.
    * @param logrec
    *   a byte buffer containing the bytes.
    * @return
    *   the LSN of the final value
    */
  def append(logrec: Array[Byte]) = synchronized {
    var boundary: Int = logpage.getInt(0);
    val recsize: Int = logrec.length;
    val bytesneeded: Int = recsize + Integer.BYTES;
    if (boundary - bytesneeded < Integer.BYTES) { // the log record doesn't fit,
      flush(); // so move to the next block.
      currentblk = appendNewBlock();
      boundary = logpage.getInt(0);
    }
    val recpos: Int = boundary - bytesneeded;

    logpage.setBytes(recpos, logrec);
    logpage.setInt(0, recpos); // the new boundary
    latestLSN += 1;
    latestLSN;
  }

  def getLastSavedLSN(): Int = {
    lastSavedLSN;
  }

  /** Initialize the bytebuffer and append it to the log file.
    */
  private def appendNewBlock(): BlockId = {
    val blk: BlockId = fm.append(logfile);
    logpage.setInt(0, fm.blockSize());
    fm.write(blk, logpage);
    blk;
  }

  /** Write the buffer to the log file.
    */
  private def flush(): Unit = {
    fm.write(currentblk, logpage);
    lastSavedLSN = latestLSN;
  }
}
