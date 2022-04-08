package simpledb.file;

import java.io._;
import java.util._;
import collection.JavaConverters._

class FileMgr(val dbDirectory: File, val blocksize: Int) {
  protected val _isNew: Boolean = !dbDirectory.exists()
  protected val openFiles: Map[String, RandomAccessFile] =
    scala.collection.mutable.HashMap().asJava

  // create the directory if the database is new
  if (_isNew)
    dbDirectory.mkdirs();

  // remove any leftover temporary tables
  for (filename <- dbDirectory.list()) {
    if (filename.startsWith("temp"))
      new File(dbDirectory, filename).delete();
  }

  def read(blk: BlockId, p: Page) = synchronized {
    try {
      val f: RandomAccessFile = getFile(blk.fileName());
      f.seek(blk.number() * blocksize);
      f.getChannel().read(p.contents());
    } catch {
      case e: IOException =>
        throw new RuntimeException("cannot read block " + blk);
    }
  }

  def write(blk: BlockId, p: Page) = synchronized {
    try {
      val f: RandomAccessFile = getFile(blk.fileName());
      f.seek(blk.number() * blocksize);
      f.getChannel().write(p.contents());
    } catch {
      case e: IOException =>
        throw new RuntimeException("cannot write block" + blk);
    }
  }

  def append(filename: String): BlockId = synchronized {
    val newblknum: Int = length(filename);
    val blk: BlockId = new BlockId(filename, newblknum);
    val b: Array[Byte] = new Array[Byte](blocksize);
    try {
      val f: RandomAccessFile = getFile(blk.fileName());
      f.seek(blk.number() * blocksize);
      f.write(b);
    } catch {
      case e: IOException =>
        throw new RuntimeException("cannot append block" + blk);
    }
    blk;
  }

  def length(filename: String): Int = {
    try {
      val f: RandomAccessFile = getFile(filename);
      (f.length() / blocksize).asInstanceOf[Int];
    } catch {
      case e: IOException =>
        throw new RuntimeException("cannot access " + filename);
    }
  }

  def isNew(): Boolean = {
    _isNew;
  }

  def blockSize(): Int = {
    blocksize;
  }

  @throws(classOf[IOException])
  private def getFile(filename: String): RandomAccessFile = {
    var f: RandomAccessFile = openFiles.get(filename);
    if (f == null) {
      val dbTable: File = new File(dbDirectory, filename);
      f = new RandomAccessFile(dbTable, "rws");
      openFiles.put(filename, f);
    }
    f;
  }
}
