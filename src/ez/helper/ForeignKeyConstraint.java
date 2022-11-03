package ez.helper;

import ez.DB;
import ez.misc.DatabaseType;

import ox.Log;

public class ForeignKeyConstraint {

  protected String foreigKeyName, sourceTable, sourceColumnName, foreignTable, foreignColumnName;

  protected ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    this.sourceTable = sourceTable;
    this.sourceColumnName = sourceColumnName;
    this.foreignTable = foreignTable;
    this.foreignColumnName = foreignColumnName;
  }

  protected ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
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

    DatabaseType databaseType = db.databaseType;

    StringBuilder sb = new StringBuilder("ALTER TABLE ");
    sb.append(databaseType.escape(sourceTable)).append(" ADD");
    sb.append(getCreationStatement(databaseType));
    Log.debug(sb.toString());
    db.execute(sb.toString());
  }

  public String getCreationStatement(DatabaseType databaseType) {
    StringBuilder sb = new StringBuilder();

    if (!foreigKeyName.isEmpty()) {
      sb.append(" CONSTRAINT ").append(foreigKeyName);
    }

    sb.append(" FOREIGN KEY (").append(databaseType.escape(sourceColumnName)).append(") REFERENCES ")
        .append(databaseType.escape(foreignTable)).append("(")
        .append(databaseType.escape(foreignColumnName)).append(")");

    return sb.toString();
  }

}
