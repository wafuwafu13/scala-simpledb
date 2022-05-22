package simpledb.plan;

import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.query.Scan;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class PlannerTest1 extends AnyFunSuite {
  test("Planner1") {
    val path: String = "./resources/plannertest1";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);
    val mdm: MetadataMgr = new MetadataMgr(true, tx);
    val qp: QueryPlanner = new BasicQueryPlanner(mdm);
    val up: UpdatePlanner = new BasicUpdatePlanner(mdm);
    val planner = new Planner(qp, up);

    var cmd: String = "create table T1(A int, B varchar(9))";
    planner.executeUpdate(cmd, tx);

    val n: Int = 200;
    println("Inserting " + n + " random records.");
    for (i <- 0 until n) {
      val a: Int = Math.round(Math.random() * 50).asInstanceOf[Int];
      val b: String = "rec" + a;
      cmd = "insert into T1(A,B) values(" + a + ", '" + b + "')";
      planner.executeUpdate(cmd, tx);
    }

    val want: Int = 10;
    val qry: String = "select B from T1 where A=" + want;
    val p: Plan = planner.createQueryPlan(qry, tx);
    val s: Scan = p.open();
    while (s.next())
      assert(s.getString("b") == "rec" + want);
    s.close();
    tx.commit();
  }
}
