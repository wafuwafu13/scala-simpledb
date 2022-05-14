package simpledb.parse;

class PredParser(val s: String) {
  private val lex: Lexer = new Lexer(s);

  def field(): String = {
    lex.eatId();
  }

  def constant(): Unit = {
    if (lex.matchStringConstant())
      lex.eatStringConstant();
    else
      lex.eatIntConstant();
  }

  def expression(): Unit = {
    if (lex.matchId())
      field();
    else
      constant();
  }

  def term(): Unit = {
    expression();
    lex.eatDelim('=');
    expression();
  }

  def predicate(): Unit = {
    term();
    if (lex.matchKeyword("and")) {
      lex.eatKeyword("and");
      predicate();
    }
  }
}
