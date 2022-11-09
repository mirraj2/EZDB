package ez.helper;

import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.normalize;

import com.google.common.base.Strings;

import ez.DB;
import ez.misc.DatabaseType;

import ox.Log;

public class ForeignKeyConstraint {

  protected String foreigKeyName, sourceTable, sourceColumnName, foreignTable, foreignColumnName;

  protected ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    this(sourceTable, sourceColumnName, foreignTable, foreignColumnName, null);
  }

  protected ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName, String foreignKeyName) {
    this.sourceTable = checkNotEmpty(normalize(sourceTable));
    this.sourceColumnName = checkNotEmpty(normalize(sourceColumnName));
    this.foreignTable = checkNotEmpty(normalize(foreignTable));
    this.foreignColumnName = checkNotEmpty(normalize(foreignColumnName));
    this.foreigKeyName = normalize(foreignKeyName);
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
    db.execute(sb.toString());
  }

  public String getCreationStatement(DatabaseType databaseType) {
    StringBuilder sb = new StringBuilder();

    if (!Strings.isNullOrEmpty(foreigKeyName)) {
      sb.append(" CONSTRAINT ").append(foreigKeyName);
    }

    sb.append(" FOREIGN KEY (").append(databaseType.escape(sourceColumnName)).append(") REFERENCES ")
        .append(databaseType.escape(foreignTable)).append("(")
        .append(databaseType.escape(foreignColumnName)).append(")");

    return sb.toString();
  }

}
