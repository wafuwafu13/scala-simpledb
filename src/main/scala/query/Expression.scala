package simpledb.query;

import simpledb.record._;

/** The interface corresponding to SQL expressions.
  * @author
  *   Edward Sciore
  */
class Expression(val ex: Any) {
  private var value: Constant = null;
  private var fldname: String = null;

  if (ex.isInstanceOf[Constant]) {
    value = ex.asInstanceOf[Constant]
  } else {
    fldname = ex.asInstanceOf[String]
  }

  /** Evaluate the expression with respect to the current record of the
    * specified scan.
    * @param s
    *   the scan
    * @return
    *   the value of the expression, as a Constant
    */
  def evaluate(s: Scan): Constant = {
    if (value != null) value else s.getVal(fldname);
  }

  /** Return true if the expression is a field reference.
    * @return
    *   true if the expression denotes a field
    */
  def isFieldName(): Boolean = {
    fldname != null;
  }

  /** Return the constant corresponding to a constant expression, or null if the
    * expression does not denote a constant.
    * @return
    *   the expression as a constant
    */
  def asConstant(): Constant = {
    value;
  }

  /** Return the field name corresponding to a constant expression, or null if
    * the expression does not denote a field.
    * @return
    *   the expression as a field name
    */
  def asFieldName(): String = {
    fldname;
  }

  /** Determine if all of the fields mentioned in this expression are contained
    * in the specified schema.
    * @param sch
    *   the schema
    * @return
    *   true if all fields in the expression are in the schema
    */
  def appliesTo(sch: Schema): Boolean = {
    if (value != null) true else sch.hasField(fldname);
  }

  override def toString(): String = {
    if (value != null) value.toString() else fldname;
  }
}
