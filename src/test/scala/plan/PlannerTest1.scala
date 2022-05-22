package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;
import org.scalatest.funsuite.AnyFunSuite;

class PlannerTest1 extends AnyFunSuite {
  test("Planner1") {
    val db = new SimpleDB("plannertest1", null, null);
    val tx: Transaction = db.newTx();
    val planner = db.getPlanner();

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
