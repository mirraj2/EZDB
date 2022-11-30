package ez.helper;

import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.normalize;

import ez.DB;
import ez.misc.DatabaseType;

public class ForeignKeyConstraint {

  public String sourceTable, sourceColumn, foreignTable, foreignColumn;
  public String foreignKeyName = "";
  public boolean cascade = false;

  public ForeignKeyConstraint(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    this.sourceTable = checkNotEmpty(normalize(sourceTable));
    this.sourceColumn = checkNotEmpty(normalize(sourceColumnName));
    this.foreignTable = checkNotEmpty(normalize(foreignTable));
    this.foreignColumn = checkNotEmpty(normalize(foreignColumnName));
  }

  public ForeignKeyConstraint onDeleteCascade() {
    cascade = true;
    return this;
  }

  public void execute(DB db) {
    // make sure this constraint is deleted if it already exists.
    db.removeForeignKey(sourceTable, sourceColumn, foreignTable, foreignColumn);

    DatabaseType type = db.databaseType;

    StringBuilder sb = new StringBuilder("ALTER TABLE ");
    sb.append(type.escape(sourceTable)).append(" ADD");
    sb.append(getCreationStatement(type));
    db.execute(sb.toString());
  }

  public String getCreationStatement(DatabaseType type) {
    StringBuilder sb = new StringBuilder();

    if (!foreignKeyName.isEmpty()) {
      sb.append(" CONSTRAINT ").append(foreignKeyName);
    }

    sb.append(" FOREIGN KEY (").append(type.escape(sourceColumn)).append(") REFERENCES ")
        .append(type.escape(foreignTable)).append("(")
        .append(type.escape(foreignColumn)).append(")");

    if (cascade) {
      sb.append("\n ON DELETE CASCADE");
    }

    return sb.toString();
  }

}
