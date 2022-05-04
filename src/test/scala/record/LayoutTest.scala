package simpledb.record;

import org.scalatest.funsuite.AnyFunSuite;

class LayoutTest extends AnyFunSuite {
  test("Layout") {
    val sch: Schema = new Schema();
    sch.addStringField("A", 9);
    sch.addIntField("B");

    val layout: Layout = new Layout(sch);
    var expect = 4;
    layout
      .getSchema()
      .getFields()
      .forEach(fldname => {
        val offset: Int = layout.offset(fldname);
        assert(offset == expect)
        expect += 13
      })
  }
}
