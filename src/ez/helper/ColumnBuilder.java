package ez.helper;

import static com.google.common.base.Preconditions.checkState;

import ez.DB;
import ez.DB.DatabaseType;
import ez.Table;

import ox.Log;

public class ColumnBuilder {

  protected String table, columnName, type, defaultValue, after;
  protected boolean index = false, unique = false, caseSensitive = true, notNull = false;

  /**
   * Whether the column should be inserted as index 0.
   */
  protected boolean first = false;

  protected DatabaseType databaseType = DatabaseType.MYSQL;

  protected ColumnBuilder(String table) {
    this.table = table;
  }

  public ColumnBuilder name(String name) {
    this.columnName = name;
    return this;
  }

  public ColumnBuilder type(Class<?> type) {
    return type(Table.getType(databaseType, type));
  }

  public ColumnBuilder type(String type) {
    this.type = type;
    return this;
  }

  public ColumnBuilder defaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
    return this;
  }

  public ColumnBuilder after(String columnName) {
    this.after = columnName;
    return this;
  }

  public ColumnBuilder first() {
    this.first = true;
    return this;
  }

  public ColumnBuilder index() {
    index = true;
    return this;
  }

  public ColumnBuilder uniqueIndex() {
    index = true;
    unique = true;
    return this;
  }

  public ColumnBuilder caseInsensitive() {
    caseSensitive = false;
    return this;
  }

  public ColumnBuilder notNull() {
    notNull = true;
    return this;
  }

  public void execute(DB db) {
    if (db.hasColumn(table, columnName)) {
      Log.warn(table + "." + columnName + " already exists.");
      return;
    }

    DatabaseType databaseType = db.databaseType;

    StringBuilder sb = new StringBuilder("ALTER TABLE ");
    sb.append(databaseType.escape(table)).append(" ADD ")
        .append(databaseType.escape(columnName)).append(" ").append(type);
    if (!caseSensitive) {
      sb.append(" COLLATE " + Table.CASE_INSENSITIVE_COLLATION);
    }
    if (defaultValue != null) {
      sb.append(" DEFAULT '").append(defaultValue).append("'");
    }
    if (notNull) {
      sb.append(" NOT NULL");
    }

    if (first) {
      checkState(databaseType != DatabaseType.POSTGRES, "Postgres doesn't support FIRST");
      sb.append(" FIRST");
    } else if (after != null) {
      checkState(databaseType != DatabaseType.POSTGRES, "Postgres doesn't support AFTER");
      sb.append(" AFTER ").append(databaseType.escape(after));
    }

    db.execute(sb.toString());

    if (index) {
      db.addIndex(table, columnName, unique);
    }
  }

  public static ColumnBuilder create(String table, String name, Class<?> type) {
    return create(table, name, Table.getType(DatabaseType.MYSQL, type));
  }

  public static ColumnBuilder create(String table, String name, String type) {
    return new ColumnBuilder(table).name(name).type(type);
  }

  public static ColumnBuilder table(String table) {
    return new ColumnBuilder(table);
  }

}