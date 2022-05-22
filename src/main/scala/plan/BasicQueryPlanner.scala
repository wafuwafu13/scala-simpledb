package simpledb.plan;

import java.util._;
import simpledb.tx.Transaction;
import simpledb.metadata._;
import simpledb.parse._;

/** The simplest, most naive query planner possible.
  * @author
  *   Edward Sciore
  */
class BasicQueryPlanner(val mdm: MetadataMgr) extends QueryPlanner {

  /** Creates a query plan as follows. It first takes the product of all tables
    * and views; it then selects on the predicate; and finally it projects on
    * the field list.
    */
  def createPlan(data: QueryData, tx: Transaction): Plan = {
    // Step 1: Create a plan for each mentioned table or view.
    val plans: List[Plan] = new ArrayList[Plan]();
    data
      .getTables()
      .forEach(tblname => {
        val viewdef: String = mdm.getViewDef(tblname, tx);
        // TODO: Why `viewdef != ""` is required
        if (viewdef != null && viewdef != "") { // Recursively plan the view.
          val parser: Parser = new Parser(viewdef);
          val viewdata: QueryData = parser.query();
          plans.add(createPlan(viewdata, tx));
        } else
          plans.add(new TablePlan(tx, tblname, mdm));
      })

    // Step 2: Create the product of all table plans
    var p: Plan = plans.remove(0);
    plans.forEach(nextplan => {
      p = new ProductPlan(p, nextplan);
    })

    // Step 3: Add a selection plan for the predicate
    p = new SelectPlan(p, data.getPred());

    // Step 4: Project on the field names
    p = new ProjectPlan(p, data.getFields());
    p;
  }
}
