package simpledb.query;

/** The class that denotes values stored in the database.
  * @author
  *   Edward Sciore
  */
class Constant(value: Any) extends Comparable[Constant] {
  val ival =
    if (value.isInstanceOf[Integer]) value
    else null

  val sval =
    if (value.isInstanceOf[String]) value
    else null

  def asInt(): Int = {
    ival.asInstanceOf[Int];
  }

  def asString(): String = {
    sval.asInstanceOf[String];
  }

  def constantequals(obj: Object): Boolean = {
    val c: Constant = obj.asInstanceOf[Constant];
    if (ival != null) ival == c.ival else sval == c.sval;
  }

  def compareTo(c: Constant): Int = {
    if (ival != null) ival.asInstanceOf[Int].compareTo(c.ival.asInstanceOf[Int])
    else sval.asInstanceOf[String].compareTo(c.sval.asInstanceOf[String]);
  }

  override def hashCode(): Int = {
    if (ival != null) ival.hashCode() else sval.hashCode();
  }

  override def toString(): String = {
    if (ival != null) ival.toString() else sval.toString();
  }
}
