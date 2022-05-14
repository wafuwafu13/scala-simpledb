package simpledb.parse

import java.util._;
import org.scalatest.funsuite.AnyFunSuite;

class ParserTest extends AnyFunSuite {
  test("Parser") {
    val lists: List[String] = Arrays.asList(
      "insert into T ( A, B, C ) values ( 'a', 'b', 'c' )",
      "delete from T where A = B",
      "update T set A = 'a' where B = C",
      "create table T ( A int, B varchar ( 9 ) )",
      "create view V as " + "select C from T where A = B",
      "create index I on T ( A )"
    )
    var p = new Parser("select C from T where A = B")
    println(p.query())
    lists.forEach(q => {
      p = new Parser(q)
      println(p.updateCmd())
    })
  }
}
