package simpledb.plan;

import simpledb.file._;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.query._;
import java.util._;
import org.scalatest.funsuite.AnyFunSuite;
import java.io.File;

class SingleTablePlanTest extends AnyFunSuite {
  def printStats(n: Int, p: Plan): Unit = {
    println("Here are the stats for plan p" + n);
    println("\tR(p" + n + "): " + p.recordsOutput());
    println("\tB(p" + n + "): " + p.blocksAccessed());
    println();
  }

  test("SingleTablePlan") {
    val path: String = "./resources/studentdb";
    val logfilename: String = "simpledb.log";
    val blocksize: Int = 400;
    val fm: FileMgr = new FileMgr(new File(path), blocksize);
    val lm = new LogMgr(fm, logfilename);
    val bm = new BufferMgr(fm, lm, 8);
    val tx: Transaction = new Transaction(fm, lm, bm);
    val mdm: MetadataMgr = new MetadataMgr(true, tx);

    // TODO: fix IndexOutOfBoundsException
    // // the STUDENT node
    // val p1: Plan = new TablePlan(tx, "student", mdm);

    // // the Select node for "major = 10"
    // val t: Term =
    //   new Term(new Expression("majorid"), new Expression(new Constant(10)));
    // val pred: Predicate = new Predicate(t);
    // val p2: Plan = new SelectPlan(p1, pred);

    // // the Select node for "gradyear = 2020"
    // val t2: Term =
    //   new Term(new Expression("gradyear"), new Expression(new Constant(2020)));
    // val pred2: Predicate = new Predicate(t2);
    // val p3: Plan = new SelectPlan(p2, pred2);

    // // the Project node
    // val c: List[String] = Arrays.asList("sname", "majorid", "gradyear");
    // val p4: Plan = new ProjectPlan(p3, c);

    // // Look at R(p) and B(p) for each plan p.
    // printStats(1, p1); printStats(2, p2); printStats(3, p3); printStats(4, p4);

    // // Change p2 to be p2, p3, or p4 to see the other scans in action.
    // // Changing p2 to p4 will throw an exception because SID is not in the projection list.
    // val s: Scan = p2.open();
    // while (s.next()) {
    //   println(
    //     s.getInt("sid") + " " + s.getString("sname")
    //       + " " + s.getInt("majorid") + " " + s.getInt("gradyear")
    //   );
    // }
    // s.close();
  }
}
