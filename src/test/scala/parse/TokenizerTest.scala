package simpledb.parse;

import java.io._;
import java.util._;
import java.io.StreamTokenizer._;
import org.scalatest.funsuite.AnyFunSuite;

class TokenizerTest extends AnyFunSuite {
  val keywords: List[String] = Arrays.asList(
    "select",
    "from",
    "where",
    "and",
    "insert",
    "into",
    "values",
    "delete",
    "update",
    "set",
    "create",
    "table",
    "int",
    "varchar",
    "view",
    "as",
    "index",
    "on"
  );

  private def printCurrentToken(tok: StreamTokenizer): Unit = {
    if (tok.ttype == TT_NUMBER)
      println("IntConstant " + tok.nval);
    else if (tok.ttype == TT_WORD) {
      val word: String = tok.sval;
      if (keywords.contains(word))
        println("Keyword " + word);
      else
        println("Id " + word);
    } else if (tok.ttype == '\'')
      println("StringConstant " + tok.sval);
    else
      println("Delimiter " + tok.ttype);
  }

  test("Tokenizer") {
    val s: String = "SELECT a FROM X, Z WHERE b = 3 AND c = 'foobar'";
    val tok: StreamTokenizer = new StreamTokenizer(new StringReader(s));
    tok.ordinaryChar('.');
    tok.lowerCaseMode(true); // ids and keywords are converted to lower case
    while (tok.nextToken() != TT_EOF)
      printCurrentToken(tok);
  }
}
