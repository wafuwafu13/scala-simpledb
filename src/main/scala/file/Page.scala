package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset._;

class Page(val blocksize: Int) {
  protected val bb = ByteBuffer.allocateDirect(blocksize);
  protected val CHARSET: Charset = StandardCharsets.US_ASCII;

  def getInt(offset: Int): Int = {
    bb.getInt(offset);
  }

  def setInt(offset: Int, n: Int): Unit = {
    bb.putInt(offset, n);
  }

  def getBytes(offset: Int): Array[Byte] = {
    bb.position(offset);
    val length: Int = bb.getInt();
    val b: Array[Byte] = new Array[Byte](length);
    bb.get(b);
    b;
  }

  def setBytes(offset: Int, b: Array[Byte]): Unit = {
    bb.position(offset);
    bb.putInt(b.length);
    bb.put(b);
  }

  def getString(offset: Int): String = {
    val b: Array[Byte] = getBytes(offset);
    new String(b, CHARSET);
  }

  def setString(offset: Int, s: String): Unit = {
    val b: Array[Byte] = s.getBytes(CHARSET);
    setBytes(offset, b);
  }

  def maxLength(strlen: Int): Int = {
    val bytesPerChar: Float = CHARSET.newEncoder().maxBytesPerChar();
    Integer.BYTES + (strlen * bytesPerChar.asInstanceOf[Int]);
  }

  // a package private method, needed by FileMgr
  def contents(): ByteBuffer = {
    bb.position(0);
    bb;
  }
}
