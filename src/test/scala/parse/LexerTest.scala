package simpledb.parse;

import java.util._;
import org.scalatest.funsuite.AnyFunSuite;

// Will successfully read in lines of text denoting an
// SQL expression of the form "id = c" or "c = id".

class LexerTest extends AnyFunSuite {
  test("Lexer") {
    val lists: List[String] = Arrays.asList(
      "A = 1",
      "3 = B"
    )
    lists.forEach(s => {
      val lex: Lexer = new Lexer(s);
      var x: String = "";
      var y: Int = 0;
      if (lex.matchId()) {
        x = lex.eatId();
        lex.eatDelim('=');
        y = lex.eatIntConstant();
      } else {
        y = lex.eatIntConstant();
        lex.eatDelim('=');
        x = lex.eatId();
      }
      println(x + " equals " + y);
    })
  }
}
