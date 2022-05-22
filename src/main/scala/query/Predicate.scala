package simpledb.query;

import java.util._;

import simpledb.plan.Plan;
import simpledb.record._;

/** A predicate is a Boolean combination of terms.
  * @author
  *   Edward Sciore
  */
class Predicate(val t: Any) {
  private val terms: List[Term] = new ArrayList[Term]();
  if (t.isInstanceOf[Term]) {
    terms.add(t.asInstanceOf[Term]);
  }

  /** Modifies the predicate to be the conjunction of itself and the specified
    * predicate.
    * @param pred
    *   the other predicate
    */
  def conjoinWith(pred: Predicate): Unit = {
    terms.addAll(pred.terms);
  }

  /** Returns true if the predicate evaluates to true with respect to the
    * specified scan.
    * @param s
    *   the scan
    * @return
    *   true if the predicate is true in the scan
    */
  def isSatisfied(s: Scan): Boolean = {
    terms.forEach(t => {
      if (!t.isSatisfied(s))
        return false;
    })
    true;
  }

  /** Calculate the extent to which selecting on the predicate reduces the
    * number of records output by a query. For example if the reduction factor
    * is 2, then the predicate cuts the size of the output in half.
    * @param p
    *   the query's plan
    * @return
    *   the integer reduction factor.
    */
  def reductionFactor(p: Plan): Int = {
    var factor: Int = 1;
    terms.forEach(t => {
      factor *= t.reductionFactor(p);
    })
    factor;
  }

  /** Return the subpredicate that applies to the specified schema.
    * @param sch
    *   the schema
    * @return
    *   the subpredicate applying to the schema
    */
  def selectSubPred(sch: Schema): Predicate = {
    var result: Predicate = new Predicate();
    terms.forEach(t => {
      if (t.appliesTo(sch))
        result.terms.add(t);
    })
    if (result.terms.size() == 0)
      null;
    else
      result;
  }

  /** Return the subpredicate consisting of terms that apply to the union of the
    * two specified schemas, but not to either schema separately.
    * @param sch1
    *   the first schema
    * @param sch2
    *   the second schema
    * @return
    *   the subpredicate whose terms apply to the union of the two schemas but
    *   not either schema separately.
    */
  def joinSubPred(sch1: Schema, sch2: Schema): Predicate = {
    val result: Predicate = new Predicate();
    val newsch: Schema = new Schema();
    newsch.addAll(sch1);
    newsch.addAll(sch2);
    terms.forEach(t => {
      if (
        !t.appliesTo(sch1) &&
        !t.appliesTo(sch2) &&
        t.appliesTo(newsch)
      )
        result.terms.add(t);

    })
    if (result.terms.size() == 0)
      null;
    else
      result;
  }

  /** Determine if there is a term of the form "F=c" where F is the specified
    * field and c is some constant. If so, the method returns that constant. If
    * not, the method returns null.
    * @param fldname
    *   the name of the field
    * @return
    *   either the constant or null
    */
  def equatesWithConstant(fldname: String): Constant = {
    terms.forEach(t => {
      val c: Constant = t.equatesWithConstant(fldname);
      if (c != null)
        return c;
    })
    null;
  }

  /** Determine if there is a term of the form "F1=F2" where F1 is the specified
    * field and F2 is another field. If so, the method returns the name of that
    * field. If not, the method returns null.
    * @param fldname
    *   the name of the field
    * @return
    *   the name of the other field, or null
    */
  def equatesWithField(fldname: String): String = {
    terms.forEach(t => {
      val s: String = t.equatesWithField(fldname);
      if (s != null)
        return s;

    })
    null;
  }

  override def toString(): String = {
    val iter: Iterator[Term] = terms.iterator();
    if (!iter.hasNext())
      return "";
    var result: String = iter.next().toString();
    while (iter.hasNext())
      result += " and " + iter.next().toString();
    result;
  }
}
