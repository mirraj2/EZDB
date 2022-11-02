package ez.impl;

import static ox.util.Functions.map;
import static ox.util.Utils.format;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;

import ez.DB;
import ez.misc.DatabaseType;

public class PostgresDB extends DB {

  public PostgresDB(String host, String user, String pass, String schema) {
    this(host, user, pass, schema, false, 10);
  }

  public PostgresDB(String host, String user, String pass, String schema, boolean ssl, int maxConnections) {
    super(DatabaseType.POSTGRES, host, user, pass, schema, ssl, maxConnections);
  }

  @Override
  public DB ensureSchemaExists() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DB usingSchema(String schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDatabase(String database) {
    execute(format("CREATE DATABASE \"{0}\" ENCODING 'UTF8'", database));
  }

  @Override
  public void createSchema(String schema) {
    execute(format("CREATE SCHEMA {0}", databaseType.escape(schema)));
  }

  @Override
  public boolean hasTable(String table) {
    return null != selectSingleRow("SELECT table_name"
        + " FROM INFORMATION_SCHEMA.tables WHERE table_catalog = ? AND table_name = ? LIMIT 1",
        schema, table);
  }

  @Override
  public boolean hasColumn(String table, String column) {
    return null != selectSingleRow("SELECT column_name"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_catalog = ? AND table_name = ? AND column_name = ? LIMIT 1",
        schema, table, column);
  }

  @Override
  protected void addIndex(String table, Collection<String> columns, boolean unique, String indexName) {
    List<String> cols = map(columns, s -> escape(s));
    String s = unique ? "UNIQUE " : "";

    execute("CREATE " + s + "INDEX " + escape(indexName) + " ON " + escape(table) + " ("
        + Joiner.on(',').join(cols) + ")");
  }

}
