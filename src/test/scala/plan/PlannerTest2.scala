package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;
import org.scalatest.funsuite.AnyFunSuite;

class PlannerTest2 extends AnyFunSuite {
  test("Planner2") {
    val db = new SimpleDB("plannertest2", null, null);
    val tx: Transaction = db.newTx();
    val planner = db.getPlanner();

    var cmd: String = "create table T1(A int, B varchar(9))";
    planner.executeUpdate(cmd, tx);
    val n: Int = 200;
    println("Inserting " + n + " records into T1.");
    for (i <- 0 until n) {
      val a: Int = i;
      val b: String = "bbb" + a;
      cmd = "insert into T1(A,B) values(" + a + ", '" + b + "')";
      planner.executeUpdate(cmd, tx);
    }

    cmd = "create table T2(C int, D varchar(9))";
    planner.executeUpdate(cmd, tx);
    println("Inserting " + n + " records into T2.");
    for (i <- 0 until n) {
      val c: Int = n - i - 1;
      val d: String = "ddd" + c;
      cmd = "insert into T2(C,D) values(" + c + ", '" + d + "')";
      planner.executeUpdate(cmd, tx);
    }

    val qry: String = "select B,D from T1,T2 where A=C";
    val p: Plan = planner.createQueryPlan(qry, tx);
    val s: Scan = p.open();
    while (s.next())
      println(s.getString("b") + " " + s.getString("d"));
    s.close();
    tx.commit();
  }
}
