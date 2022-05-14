package simpledb.parse;

import java.util._;
import org.scalatest.funsuite.AnyFunSuite;

class PredParserTest extends AnyFunSuite {
  test("PredParser") {
    val lists: List[String] = Arrays.asList(
      "A = B",
      "A B"
    )
    lists.forEach(s => {
      val p: PredParser = new PredParser(s);
      try {
        p.predicate();
        println("yes");
      } catch {
        case e: BadSyntaxException =>
          println("no");
      }
    })
  }
}
