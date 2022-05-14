package simpledb.parse;

import java.util._;
import simpledb.query._;
import simpledb.record._;

/** The SimpleDB parser.
  * @author
  *   Edward Sciore
  */
class Parser(val s: String) {
  private val lex: Lexer = new Lexer(s);

// Methods for parsing predicates, terms, expressions, constants, and fields

  def field(): String = {
    lex.eatId();
  }

  def constant(): Constant = {
    if (lex.matchStringConstant())
      new Constant(lex.eatStringConstant());
    else
      new Constant(lex.eatIntConstant());
  }

  def expression(): Expression = {
    if (lex.matchId())
      new Expression(field());
    else
      new Expression(constant());
  }

  def term(): Term = {
    val lhs: Expression = expression();
    lex.eatDelim('=');
    val rhs: Expression = expression();
    new Term(lhs, rhs);
  }

  def predicate(): Predicate = {
    val pred: Predicate = new Predicate(term());
    if (lex.matchKeyword("and")) {
      lex.eatKeyword("and");
      pred.conjoinWith(predicate());
    }
    pred;
  }

// Methods for parsing queries

  def query(): QueryData = {
    lex.eatKeyword("select");
    val fields: List[String] = selectList();
    lex.eatKeyword("from");
    val tables: Collection[String] = tableList();
    var pred: Predicate = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    new QueryData(fields, tables, pred);
  }

  private def selectList(): List[String] = {
    val L: ArrayList[String] = new ArrayList[String]();
    L.add(field());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(selectList());
    }
    L;
  }

  private def tableList(): Collection[String] = {
    val L: Collection[String] = new ArrayList[String]();
    L.add(lex.eatId());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(tableList());
    }
    L;
  }

// Methods for parsing the various update commands

  def updateCmd(): Object = {
    if (lex.matchKeyword("insert"))
      insert();
    else if (lex.matchKeyword("delete"))
      delete();
    else if (lex.matchKeyword("update"))
      modify();
    else
      create();
  }

  private def create(): Object = {
    lex.eatKeyword("create");
    if (lex.matchKeyword("table"))
      createTable();
    else if (lex.matchKeyword("view"))
      createView();
    else
      createIndex();
  }

// Method for parsing delete commands

  def delete(): DeleteData = {
    lex.eatKeyword("delete");
    lex.eatKeyword("from");
    val tblname: String = lex.eatId();
    var pred: Predicate = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    new DeleteData(tblname, pred);
  }

// Methods for parsing insert commands

  def insert(): InsertData = {
    lex.eatKeyword("insert");
    lex.eatKeyword("into");
    val tblname: String = lex.eatId();
    lex.eatDelim('(');
    val flds: List[String] = fieldList();
    lex.eatDelim(')');
    lex.eatKeyword("values");
    lex.eatDelim('(');
    val vals: List[Constant] = constList();
    lex.eatDelim(')');
    new InsertData(tblname, flds, vals);
  }

  private def fieldList(): List[String] = {
    val L: List[String] = new ArrayList[String]();
    L.add(field());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(fieldList());
    }
    L;
  }

  private def constList(): List[Constant] = {
    val L: List[Constant] = new ArrayList[Constant]();
    L.add(constant());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(constList());
    }
    L;
  }

// Method for parsing modify commands

  def modify(): ModifyData = {
    lex.eatKeyword("update");
    val tblname: String = lex.eatId();
    lex.eatKeyword("set");
    val fldname: String = field();
    lex.eatDelim('=');
    val newval: Expression = expression();
    var pred: Predicate = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    new ModifyData(tblname, fldname, newval, pred);
  }

// Method for parsing create table commands

  def createTable(): CreateTableData = {
    lex.eatKeyword("table");
    val tblname: String = lex.eatId();
    lex.eatDelim('(');
    val sch: Schema = fieldDefs();
    lex.eatDelim(')');
    new CreateTableData(tblname, sch);
  }

  private def fieldDefs(): Schema = {
    val schema: Schema = fieldDef();
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      val schema2: Schema = fieldDefs();
      schema.addAll(schema2);
    }
    schema;
  }

  private def fieldDef(): Schema = {
    val fldname: String = field();
    fieldType(fldname);
  }

  private def fieldType(fldname: String): Schema = {
    val schema: Schema = new Schema();
    if (lex.matchKeyword("int")) {
      lex.eatKeyword("int");
      schema.addIntField(fldname);
    } else {
      lex.eatKeyword("varchar");
      lex.eatDelim('(');
      val strLen: Int = lex.eatIntConstant();
      lex.eatDelim(')');
      schema.addStringField(fldname, strLen);
    }
    schema;
  }

// Method for parsing create view commands

  def createView(): CreateViewData = {
    lex.eatKeyword("view");
    val viewname: String = lex.eatId();
    lex.eatKeyword("as");
    val qd: QueryData = query();
    new CreateViewData(viewname, qd);
  }

//  Method for parsing create index commands

  def createIndex(): CreateIndexData = {
    lex.eatKeyword("index");
    val idxname: String = lex.eatId();
    lex.eatKeyword("on");
    val tblname: String = lex.eatId();
    lex.eatDelim('(');
    val fldname: String = field();
    lex.eatDelim(')');
    new CreateIndexData(idxname, tblname, fldname);
  }
}
