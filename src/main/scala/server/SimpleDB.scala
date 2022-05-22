package simpledb.server;

import java.io.File;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.buffer.BufferMgr;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.plan._;
// import simpledb.index.planner.IndexUpdatePlanner;
// import simpledb.opt.HeuristicQueryPlanner;

/** The class that configures the system.
  *
  * @author
  *   Edward Sciore
  */
class SimpleDB(val dirname: String, val _blocksize: Any, val _buffsize: Any) {
  val BLOCK_SIZE: Int = 400;
  val BUFFER_SIZE: Int = 8;
  val LOG_FILE: String = "simpledb.log";

  val dbDirectory: File = new File("resources/" + dirname);
  val blocksize =
    if (_blocksize != null) _blocksize
    else BLOCK_SIZE;
  val buffsize =
    if (_blocksize != null) _buffsize
    else BUFFER_SIZE;
  val fm = new FileMgr(dbDirectory, blocksize.asInstanceOf[Int]);
  val lm = new LogMgr(fm, LOG_FILE);
  val bm = new BufferMgr(fm, lm, buffsize.asInstanceOf[Int]);

  var tx: Transaction = null;
  var mdm: MetadataMgr = null;
  var planner: Planner = null;
  var qp: QueryPlanner = null;
  var up: UpdatePlanner = null;
  if (_blocksize == null && _buffsize == null) {
    tx = newTx();
    val isnew: Boolean = fm.isNew();
    if (isnew)
      println("creating new database");
    else {
      println("recovering existing database");
      tx.recover();
    }
    mdm = new MetadataMgr(isnew, tx);
    qp = new BasicQueryPlanner(mdm);
    up = new BasicUpdatePlanner(mdm);
//    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
//    UpdatePlanner up = new IndexUpdatePlanner(mdm);
    planner = new Planner(qp, up);
    tx.commit();
  }

  /** A convenient way for clients to create transactions and access the
    * metadata.
    */
  def newTx(): Transaction = {
    new Transaction(fm, lm, bm);
  }

  def mdMgr(): MetadataMgr = {
    mdm;
  }

  def getPlanner(): Planner = {
    planner;
  }

  // These methods aid in debugging
  def fileMgr(): FileMgr = {
    fm;
  }
  def logMgr(): LogMgr = {
    lm;
  }
  def bufferMgr(): BufferMgr = {
    bm;
  }
}
