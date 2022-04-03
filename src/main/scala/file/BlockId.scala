package simpledb.file

class BlockId(val filename: String, val blknum: Int) {
  def fileName(): String = {
    filename
  }

  def number(): Int = {
    blknum
  }

  def blkEquals(obj: Object): Boolean = {
    val blk: BlockId = obj.asInstanceOf[BlockId];
    filename.equals(blk.filename) && blknum == blk.blknum;
  }

  override def toString(): String = {
    "[file " + filename + ", block " + blknum + "]";
  }

  override def hashCode(): Int = {
    toString().hashCode();
  }
}
