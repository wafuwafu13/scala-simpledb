package simpledb.query;

// import simpledb.plan.Plan;
import simpledb.record._;

/** A term is a comparison between two expressions.
  * @author
  *   Edward Sciore
  */
/** Create a new term that compares two expressions for equality.
  * @param lhs
  *   the LHS expression
  * @param rhs
  *   the RHS expression
  */
class Term(val lhs: Expression, val rhs: Expression) {

  /** Return true if both of the term's expressions evaluate to the same
    * constant, with respect to the specified scan.
    * @param s
    *   the scan
    * @return
    *   true if both expressions have the same value in the scan
    */
  def isSatisfied(s: Scan): Boolean = {
    val lhsval: Constant = lhs.evaluate(s);
    val rhsval: Constant = rhs.evaluate(s);
    rhsval.isequals(lhsval);
  }

  /** Calculate the extent to which selecting on the term reduces the number of
    * records output by a query. For example if the reduction factor is 2, then
    * the term cuts the size of the output in half.
    * @param p
    *   the query's plan
    * @return
    *   the integer reduction factor.
    */
  // def reductionFactor(p: Plan): Int = {
  //    var lhsName: String = "";
  //    var rhsName: String = "";
  //    if (lhs.isFieldName() && rhs.isFieldName()) {
  //       lhsName = lhs.asFieldName();
  //       rhsName = rhs.asFieldName();
  //       return Math.max(p.distinctValues(lhsName),
  //                       p.distinctValues(rhsName));
  //    }
  //    if (lhs.isFieldName()) {
  //       lhsName = lhs.asFieldName();
  //       return p.distinctValues(lhsName);
  //    }
  //    if (rhs.isFieldName()) {
  //       rhsName = rhs.asFieldName();
  //       return p.distinctValues(rhsName);
  //    }
  //    // otherwise, the term equates constants
  //    if (lhs.asConstant().equals(rhs.asConstant()))
  //       1;
  //    else
  //       Integer.MAX_VALUE;
  // }

  /** Determine if this term is of the form "F=c" where F is the specified field
    * and c is some constant. If so, the method returns that constant. If not,
    * the method returns null.
    * @param fldname
    *   the name of the field
    * @return
    *   either the constant or null
    */
  def equatesWithConstant(fldname: String): Constant = {
    if (
      lhs.isFieldName() &&
      lhs.asFieldName().equals(fldname) &&
      !rhs.isFieldName()
    )
      return rhs.asConstant();
    else if (
      rhs.isFieldName() &&
      rhs.asFieldName().equals(fldname) &&
      !lhs.isFieldName()
    )
      return lhs.asConstant();
    else
      null;
  }

  /** Determine if this term is of the form "F1=F2" where F1 is the specified
    * field and F2 is another field. If so, the method returns the name of that
    * field. If not, the method returns null.
    * @param fldname
    *   the name of the field
    * @return
    *   either the name of the other field, or null
    */
  def equatesWithField(fldname: String): String = {
    if (
      lhs.isFieldName() &&
      lhs.asFieldName().equals(fldname) &&
      rhs.isFieldName()
    )
      return rhs.asFieldName();
    else if (
      rhs.isFieldName() &&
      rhs.asFieldName().equals(fldname) &&
      lhs.isFieldName()
    )
      return lhs.asFieldName();
    else
      null;
  }

  /** Return true if both of the term's expressions apply to the specified
    * schema.
    * @param sch
    *   the schema
    * @return
    *   true if both expressions apply to the schema
    */
  def appliesTo(sch: Schema): Boolean = {
    lhs.appliesTo(sch) && rhs.appliesTo(sch);
  }

  override def toString(): String = {
    lhs.toString() + "=" + rhs.toString();
  }
}
