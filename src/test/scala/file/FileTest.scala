package simpledb.file

import org.scalatest.funsuite.AnyFunSuite
import java.io.File;

class FileTest extends AnyFunSuite {
  test("File") {
    val path: String = "./resources/filetest"
    val blockSize: Int = 400
    val fm: FileMgr = new FileMgr(new File(path), 400)
    val blk: BlockId = new BlockId("testfile", 2);
    val pos1: Int = 88;

    val p1: Page = new Page(fm.blockSize());
    p1.setString(pos1, "abcdefghijklm");
    val size: Int = p1.maxLength("abcdefghijklm".length());
    val pos2: Int = pos1 + size;
    p1.setInt(pos2, 345);
    fm.write(blk, p1);

    val p2: Page = new Page(fm.blockSize());
    fm.read(blk, p2);
    assert(345 == p2.getInt(pos2));
    assert("abcdefghijklm" == p2.getString(pos1))
  }
}
