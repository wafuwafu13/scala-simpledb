package simpledb.parse;

import java.util._;
import java.io._;

/** The lexical analyzer.
  * @author
  *   Edward Sciore
  */
class Lexer(val s: String) {
  private val keywords: List[String] = Arrays.asList(
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

  /** Creates a new lexical analyzer for SQL statement s.
    * @param s
    *   the SQL statement
    */

  private val tok: StreamTokenizer = new StreamTokenizer(new StringReader(s));
  tok.ordinaryChar('.'); // disallow "." in identifiers
  tok.wordChars('_', '_'); // allow "_" in identifiers
  tok.lowerCaseMode(true); // ids and keywords are converted
  nextToken();

//Methods to check the status of the current token

  /** Returns true if the current token is the specified delimiter character.
    * @param d
    *   a character denoting the delimiter
    * @return
    *   true if the delimiter is the current token
    */
  def matchDelim(d: Char): Boolean = {
    d == tok.ttype;
  }

  /** Returns true if the current token is an integer.
    * @return
    *   true if the current token is an integer
    */
  def matchIntConstant(): Boolean = {
    tok.ttype == StreamTokenizer.TT_NUMBER;
  }

  /** Returns true if the current token is a string.
    * @return
    *   true if the current token is a string
    */
  def matchStringConstant(): Boolean = {
    '\'' == tok.ttype;
  }

  /** Returns true if the current token is the specified keyword.
    * @param w
    *   the keyword string
    * @return
    *   true if that keyword is the current token
    */
  def matchKeyword(w: String): Boolean = {
    tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
  }

  /** Returns true if the current token is a legal identifier.
    * @return
    *   true if the current token is an identifier
    */
  def matchId(): Boolean = {
    tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
  }

//Methods to "eat" the current token

  /** Throws an exception if the current token is not the specified delimiter.
    * Otherwise, moves to the next token.
    * @param d
    *   a character denoting the delimiter
    */
  def eatDelim(d: Char): Unit = {
    if (!matchDelim(d))
      throw new BadSyntaxException();
    nextToken();
  }

  /** Throws an exception if the current token is not an integer. Otherwise,
    * returns that integer and moves to the next token.
    * @return
    *   the integer value of the current token
    */
  def eatIntConstant(): Int = {
    if (!matchIntConstant())
      throw new BadSyntaxException();
    val i: Int = tok.nval.asInstanceOf[Int];
    nextToken();
    i;
  }

  /** Throws an exception if the current token is not a string. Otherwise,
    * returns that string and moves to the next token.
    * @return
    *   the string value of the current token
    */
  def eatStringConstant(): String = {
    if (!matchStringConstant())
      throw new BadSyntaxException();
    val s: String = tok.sval; // constants are not converted to lower case
    nextToken();
    s;
  }

  /** Throws an exception if the current token is not the specified keyword.
    * Otherwise, moves to the next token.
    * @param w
    *   the keyword string
    */
  def eatKeyword(w: String): Unit = {
    if (!matchKeyword(w))
      throw new BadSyntaxException();
    nextToken();
  }

  /** Throws an exception if the current token is not an identifier. Otherwise,
    * returns the identifier string and moves to the next token.
    * @return
    *   the string value of the current token
    */
  def eatId(): String = {
    if (!matchId())
      throw new BadSyntaxException();
    val s: String = tok.sval;
    nextToken();
    s;
  }

  private def nextToken(): Unit = {
    try {
      tok.nextToken();
    } catch {
      case e: IOException =>
        throw new BadSyntaxException();
    }
  }
}
