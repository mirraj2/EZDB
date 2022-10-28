package ez.helper;

import ez.DB;

import ox.Log;

public class ForeignKeyBuilder {

  protected String foreigKeyName, sourceTable, sourceColumnName, foreignTable, foreignColumnName;

  protected ForeignKeyBuilder(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    this.sourceTable = sourceTable;
    this.sourceColumnName = sourceColumnName;
    this.foreignTable = foreignTable;
    this.foreignColumnName = foreignColumnName;
  }

  public ForeignKeyBuilder name(String foreignKeyName) {
    this.foreigKeyName = foreignKeyName;
    return this;
  }

  public void execute(DB db) {
    if (db.hasForeignKey(sourceTable, sourceColumnName, foreignTable, foreignColumnName)) {
      Log.warn("Foreign Key for " + sourceTable + "." + sourceColumnName + " referencing " + foreignTable + "."
          + foreignColumnName + " already exists.");
      return;
    }

    StringBuilder sb = new StringBuilder("ALTER TABLE `");
    sb.append(sourceTable).append("` ADD");

    if (!foreigKeyName.isEmpty()) {
      sb.append(" CONSTRAINT ").append(foreigKeyName);
    }

    sb.append(" FOREIGN KEY (`").append(sourceColumnName).append("`) REFERENCES `").append(foreignTable).append("`(`")
        .append(foreignColumnName).append("`)");

    db.execute(sb.toString());
  }

  public static ForeignKeyBuilder create(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    return new ForeignKeyBuilder(sourceTable, sourceColumnName, foreignTable, foreignColumnName);
  }

  public static ForeignKeyBuilder create(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName, String foreignKeyName) {
    return new ForeignKeyBuilder(sourceTable, sourceColumnName, foreignTable, foreignColumnName).name(foreignKeyName);
  }

}
