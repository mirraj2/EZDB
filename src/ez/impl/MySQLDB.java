package ez.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static ox.util.Functions.map;
import static ox.util.Utils.format;
import static ox.util.Utils.normalize;
import static ox.util.Utils.propagate;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;

import ez.DB;
import ez.misc.DatabaseType;

import ox.Log;

public class MySQLDB extends DB {

  protected MySQLDB(String schema) {
    super(DatabaseType.MYSQL, schema);
  }

  public MySQLDB(String host, String user, String pass, String schema) {
    this(host, user, pass, schema, false, 10);
  }

  public MySQLDB(String host, String user, String pass, String schema, boolean ssl,
      int maxConnections) {
    super(DatabaseType.MYSQL, host, user, pass, schema, ssl, maxConnections);
  }

  /**
   * Creates the schema if it doesn't already exist.
   */
  @Override
  public DB ensureSchemaExists() {
    Connection connection = null;
    try {
      connection = getConnection();
      close(connection);
    } catch (Exception e) {
      if (!e.getMessage().contains("Unknown database")) {
        throw propagate(e);
      }
      Log.info("Creating schema: " + schema);
      DB temp = new MySQLDB(host, user, pass, "", ssl, maxConnections);
      temp.createSchema(schema);
      temp.shutdown();
    }
    return this;
  }

  @Override
  public DB usingSchema(String schema) {
    schema = normalize(schema);
    if (!schema.isEmpty()) {
      checkArgument(isValidName(schema));
      if (!getSchemas().contains(schema.toLowerCase())) {
        createSchema(schema);
      }
    }
    return new MySQLDB(host, user, pass, schema, ssl, maxConnections);
  }

  @Override
  public void createDatabase(String database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createSchema(String schema) {
    execute(format("CREATE DATABASE `{0}` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_bin", schema));
  }

  @Override
  public boolean hasTable(String table) {
    return null != selectSingleRow("SELECT `COLUMN_NAME`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1",
        schema, table);
  }

  @Override
  public boolean hasColumn(String table, String column) {
    return null != selectSingleRow("SELECT `COLUMN_NAME`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1",
        schema, table, column);
  }

  @Override
  public boolean hasForeignKey(String sourceTable, String sourceColumn, String foreignTable, String foreignColumn) {
    return null != selectSingleRow("SELECT `COLUMN_NAME`"
        + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND"
        + " REFERENCED_TABLE_NAME = ? AND REFERENCED_COLUMN_NAME = ? LIMIT 1",
        schema, sourceTable, sourceColumn, foreignTable, foreignColumn);
  }

  @Override
  public String getForeignKeyName(String sourceTable, String sourceColumn, String foreignTable, String foreignColumn) {
    return selectSingleRow("SELECT `CONSTRAINT_NAME`"
        + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?"
        + " AND REFERENCED_TABLE_NAME = ? AND REFERENCED_COLUMN_NAME = ? LIMIT 1",
        schema, sourceTable, sourceColumn, foreignTable, foreignColumn).get("CONSTRAINT_NAME");
  }

  @Override
  protected void addIndex(String table, Collection<String> columns, boolean unique, String indexName) {
    List<String> cols = map(columns, s -> escape(s));
    String s = unique ? "ADD UNIQUE INDEX" : "ADD INDEX";
    execute("ALTER TABLE `" + table + "` " + s + " `" + indexName + "` (" + Joiner.on(",").join(cols) + ")");
  }

}
