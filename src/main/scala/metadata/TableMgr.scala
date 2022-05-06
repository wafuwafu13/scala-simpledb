package simpledb.metadata;

import java.util._;
import simpledb.tx.Transaction;
import simpledb.record._;
import collection.JavaConverters._

/** The table manager. There are methods to create a table, save the metadata in
  * the catalog, and obtain the metadata of a previously-created table.
  * @author
  *   Edward Sciore
  */
class TableMgr(val isNew: Boolean, val tx: Transaction) {
  // The max characters a tablename or fieldname can have.
  private val MAX_NAME: Int = 16;

  /** Create a new catalog manager for the database system. If the database is
    * new, the two catalog tables are created.
    * @param isNew
    *   has the value true if the database is new
    * @param tx
    *   the startup transaction
    */
  val tcatSchema: Schema = new Schema();
  tcatSchema.addStringField("tblname", MAX_NAME);
  tcatSchema.addIntField("slotsize");
  private val tcatLayout: Layout = new Layout(tcatSchema, null, null);

  val fcatSchema: Schema = new Schema();
  fcatSchema.addStringField("tblname", MAX_NAME);
  fcatSchema.addStringField("fldname", MAX_NAME);
  fcatSchema.addIntField("type");
  fcatSchema.addIntField("length");
  fcatSchema.addIntField("offset");
  private val fcatLayout: Layout = new Layout(fcatSchema, null, null);

  if (isNew) {
    createTable("tblcat", tcatSchema, tx);
    createTable("fldcat", fcatSchema, tx);
  }

  /** Create a new table having the specified name and schema.
    * @param tblname
    *   the name of the new table
    * @param sch
    *   the table's schema
    * @param tx
    *   the transaction creating the table
    */
  def createTable(tblname: String, sch: Schema, tx: Transaction): Unit = {
    val layout: Layout = new Layout(sch, null, null);
    // insert one record into tblcat
    val tcat: TableScan = new TableScan(tx, "tblcat", tcatLayout);
    tcat.insert();
    tcat.setString("tblname", tblname);
    tcat.setInt("slotsize", layout.slotSize());
    tcat.close();

    // insert a record into fldcat for each field
    val fcat: TableScan = new TableScan(tx, "fldcat", fcatLayout);
    sch
      .getFields()
      .forEach(fldname => {
        fcat.insert();
        fcat.setString("tblname", tblname);
        fcat.setString("fldname", fldname);
        fcat.setInt("type", sch.getType(fldname));
        fcat.setInt("length", sch.length(fldname));
        fcat.setInt("offset", layout.offset(fldname));

      })
    fcat.close();
  }

  /** Retrieve the layout of the specified table from the catalog.
    * @param tblname
    *   the name of the table
    * @param tx
    *   the transaction
    * @return
    *   the table's stored metadata
    */
  def getLayout(tblname: String, tx: Transaction): Layout = {
    var size: Int = -1;
    val tcat: TableScan = new TableScan(tx, "tblcat", tcatLayout);
    var flag: Boolean = true;
    while (tcat.next() && flag)
      if (tcat.getString("tblname").equals(tblname)) {
        size = tcat.getInt("slotsize");
        flag = false;
      }
    tcat.close();

    val sch: Schema = new Schema();
    val offsets: Map[String, Integer] =
      scala.collection.mutable.HashMap().asJava

    val fcat: TableScan = new TableScan(tx, "fldcat", fcatLayout);
    while (fcat.next())
      if (fcat.getString("tblname").equals(tblname)) {
        val fldname: String = fcat.getString("fldname");
        val fldtype: Int = fcat.getInt("type");
        val fldlen: Int = fcat.getInt("length");
        val offset: Int = fcat.getInt("offset");
        offsets.put(fldname, offset);
        sch.addField(fldname, fldtype, fldlen);
      }
    fcat.close();
    return new Layout(sch, offsets, size);
  }
}
