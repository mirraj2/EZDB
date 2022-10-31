package ez.helper;

import ez.DB;

import ox.Log;

public class ForeignKeyConstraint {

  protected String foreigKeyName, sourceTable, sourceColumnName, foreignTable, foreignColumnName;

  public ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    this.sourceTable = sourceTable;
    this.sourceColumnName = sourceColumnName;
    this.foreignTable = foreignTable;
    this.foreignColumnName = foreignColumnName;
  }

  public ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName, String foreignKeyName) {
    this.sourceTable = sourceTable;
    this.sourceColumnName = sourceColumnName;
    this.foreignTable = foreignTable;
    this.foreignColumnName = foreignColumnName;
    this.foreigKeyName = foreignKeyName;
  }

  public void execute(DB db) {
    if (db.hasForeignKey(sourceTable, sourceColumnName, foreignTable, foreignColumnName)) {
      Log.warn("Foreign Key for " + sourceTable + "." + sourceColumnName + " referencing " + foreignTable + "."
          + foreignColumnName + " already exists.");
      return;
    }

    StringBuilder sb = new StringBuilder("ALTER TABLE `");
    sb.append(sourceTable).append("` ADD");
    sb.append(getCreationStatement());
    db.execute(sb.toString());
  }

  public String getCreationStatement() {
    StringBuilder sb = new StringBuilder();

    if (!foreigKeyName.isEmpty()) {
      sb.append(" CONSTRAINT ").append(foreigKeyName);
    }

    sb.append(" FOREIGN KEY (`").append(sourceColumnName).append("`) REFERENCES `").append(foreignTable).append("`(`")
        .append(foreignColumnName).append("`)");

    return sb.toString();
  }

}
