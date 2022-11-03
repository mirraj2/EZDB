package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static ox.util.Utils.abbreviate;
import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.first;
import static ox.util.Utils.format;
import static ox.util.Utils.normalize;
import static ox.util.Utils.only;
import static ox.util.Utils.propagate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;

import ez.RowInserter.ReplaceOptions;
import ez.Table.Index;
import ez.helper.DebuggingData;
import ez.misc.DatabaseType;
import ez.misc.IsolationLevel;
import ez.misc.RollbackException;

import ox.Json;
import ox.Log;
import ox.Money;
import ox.Percent;
import ox.Reflection;
import ox.x.XList;
import ox.x.XOptional;
import ox.x.XSet;

public abstract class DB {

  public static boolean debug = false;
  public static int maxDebugLength = 1000;

  /**
   * Used to indicate that a value should be inserted as 'null' when calling insertRawRows()
   */
  public static final String NULL = "ez.DB.NULL";

  private final HikariDataSource source;

  protected final InheritableThreadLocal<Connection> transactionConnections = new InheritableThreadLocal<>();
  private final InheritableThreadLocal<DebuggingData> threadDebuggingData = new InheritableThreadLocal<>();
  private final InheritableThreadLocal<Boolean> disableForeignKeyChecks = new InheritableThreadLocal<>();

  public final DatabaseType databaseType;
  public final String host, user, pass;
  public final String catalog, schema;
  public final boolean ssl;

  protected final int maxConnections;

  protected DB(DatabaseType databaseType, String schema) {
    this.databaseType = databaseType;
    host = user = pass = "";
    if (databaseType == DatabaseType.POSTGRES) {
      this.catalog = schema;
      this.schema = "public";
    } else {
      this.catalog = "";
      this.schema = schema;
    }
    ssl = false;
    source = null;
    this.maxConnections = 10;
  }

  public DB(DatabaseType databaseType, String host, String user, String pass, String schema, boolean ssl,
      int maxConnections) {
    this.databaseType = checkNotNull(databaseType);
    this.host = host;
    this.user = user;
    this.pass = pass;

    schema = normalize(schema);
    if (databaseType == DatabaseType.POSTGRES) {
      this.catalog = schema;
      this.schema = "public";
    } else {
      this.catalog = "";
      this.schema = schema;
    }

    this.ssl = ssl;
    this.maxConnections = maxConnections;

    try {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver());
    } catch (SQLException e) {
      throw propagate(e);
    }

    try {
      DriverManager.registerDriver(new org.postgresql.Driver());
    } catch (SQLException e) {
      throw propagate(e);
    }

    String type = databaseType == DatabaseType.POSTGRES ? "postgresql" : "mysql";
    int port = databaseType == DatabaseType.POSTGRES ? 5432 : 3306;

    String url = format("jdbc:{0}://{1}:{2}/{3}", type, host, port, schema);

    if (databaseType == DatabaseType.MYSQL) {
      if (ssl) {
        url += "?requireSSL=true&useSSL=true&verifyServerCertificate=true";
      } else {
        url += "?useSSL=false";
      }
      // url += "&useLegacyDatetimeCode=false";
      // url += "&serverTimezone=UTC";
      url += "&characterEncoding=utf8";
    } else if (databaseType == DatabaseType.POSTGRES) {
      // url += "?adaptiveFetch=true&defaultRowFetchSize=64&maxResultBuffer=128M";
    }

    if (debug) {
      Log.debug(url);
    }

    source = new HikariDataSource();
    source.setJdbcUrl(url);
    source.setUsername(user);
    source.setPassword(pass);
    source.setMaximumPoolSize(maxConnections);
    // source.setConnectionInitSql("SET NAMES utf8mb4");
    source.setAutoCommit(true);
  }

  public String escape(String s) {
    return databaseType.escape(s);
  }

  public abstract DB ensureSchemaExists();

  public abstract DB usingSchema(String schema);

  /**
   * Imports a table into the current schema.
   */
  public DB importTable(String fromSchema, String tableName) {
    execute("RENAME TABLE `" + fromSchema + "`.`" + tableName + "` TO `" + tableName + "`");
    return this;
  }

  public abstract void createDatabase(String database);

  public abstract void createSchema(String schema);

  /**
   * Checks if this is a valid name for a table, column, etc. Must consist of only letters, numbers, and underscores.
   */
  public static boolean isValidName(String name) {
    return Pattern.matches("^[a-z][a-z0-9_.]*$", name);
  }

  public DB disableForeignKeyChecks(Runnable r) {
    disableForeignKeyChecks.set(true);
    try {
      r.run();
    } finally {
      disableForeignKeyChecks.set(false);
    }
    return this;
  }

  public DB transaction(Runnable r) {
    return transaction(r, IsolationLevel.SERIALIZABLE);
  }

  public DB transaction(Runnable r, IsolationLevel isolationLevel) {
    if (isInTransaction()) {
      // if we're already in a transaction, we can just call the callback. If an exception is thrown, the outer
      // transaction will roll everything back.
      r.run();
      return this;
    }

    Connection conn = getConnection();
    try {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(isolationLevel.level);
      transactionConnections.set(conn);
      r.run();
      conn.commit();
    } catch (Exception e) {
      try {
        if (shouldRollback(e)) {
          Log.error("DB: Exception occurred, rolling back transaction.");
          conn.rollback();
        } else {
          Log.error("DB: Exception occurred, but NOT rolling back transaction");
        }
      } catch (Exception ee) {
        throw propagate(ee);
      }
      throw propagate(e);
    } finally {
      transactionConnections.remove();
      try {
        conn.setAutoCommit(true);
        conn.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return this;
  }

  private boolean shouldRollback(Exception e) {
    Throwable cause = Throwables.getRootCause(e);
    if (cause instanceof RollbackException) {
      return ((RollbackException) cause).shouldRollback();
    } else {
      return true;
    }
  }

  private boolean isInTransaction() {
    return transactionConnections.get() != null;
  }

  public Row selectSingleRow(String query, Object... args) {
    List<Row> rows = select(query, args);
    if (rows.size() > 1) {
      Log.debug("query: " + query);
      Log.debug("args: " + Arrays.toString(args));
      if (rows.size() < 10) {
        for (Row row : rows) {
          Log.debug(row);
        }
      }
      throw new IllegalStateException("Expected one row, but found " + rows.size());
    }
    return only(rows);
  }

  public XList<Row> select(String query, Object... args) {
    XList<Row> ret = XList.create();
    stream(query, XOptional.empty(), false, row -> {
      ret.add(row);
    }, args);
    return ret;
  }

  @SuppressWarnings("unchecked")
  public <T> XList<T> selectSingleColumn(String query, Object... args) {
    XList<T> ret = new XList<>();
    select(query, r -> {
      try {
        ret.add((T) r.getObject(1));
      } catch (SQLException e) {
        throw propagate(e);
      }
    }, XOptional.empty(), args);
    return ret;
  }

  private void select(String query, Consumer<ResultSet> rowCallback, XOptional<Integer> fetchSize, Object... args) {
    new RowSelector().select(this, query, rowCallback, fetchSize, args);
  }

  public void stream(String query, Consumer<Row> callback, Object... args) {
    stream(query, XOptional.empty(), true, callback, args);
  }

  public void stream(String query, int fetchSize, Consumer<Row> callback, Object... args) {
    stream(query, XOptional.of(fetchSize), true, callback, args);
  }

  public void stream(String query, XOptional<Integer> fetchSize, boolean reuseRows, Consumer<Row> callback,
      Object... args) {
    new RowSelector().stream(this, query, fetchSize, reuseRows, callback, args);
  }

  public void streamBulk(String query, Consumer<XList<Row>> callback, int chunkSize, Object... args) {
    int offset = 0;

    while (true) {
      XList<Row> rows = select(query + " LIMIT " + chunkSize + " OFFSET " + offset, args);
      callback.accept(rows);
      offset += chunkSize;
      if (rows.size() < chunkSize) {
        break;
      }
    }
  }

  /**
   * Returns the id of the inserted row.
   */
  public Long insert(Table table, Row row) {
    insert(table, ImmutableList.of(row));
    Object o = row.getObject("id");
    if (o instanceof Long) {
      return (Long) o;
    }
    return null;
  }

  public void insertRawRows(String table, List<? extends Iterable<?>> rows) {
    new RowInserter().insertRawRows(this, table, rows);
  }

  public void replace(Table table, Row row) {
    replace(table, ImmutableList.of(row));
  }

  /**
   * REPLACE works exactly like INSERT, except that the old row is deleted before the new row is inserted (based on
   * primary key or unique index)
   */
  public void replace(Table table, List<Row> rows) {
    replace(table, rows, new ReplaceOptions("", XSet.create()));
  }

  public void replace(Table table, List<Row> rows, ReplaceOptions replaceOptions) {
    insert(table, rows, 16_000, XOptional.of(replaceOptions));
  }

  public void insert(Table table, List<Row> rows) {
    insert(table, rows, 16_000);
  }

  public void insert(String tableName, List<Row> rows, int chunkSize) {
    insert(new Table(tableName), rows, chunkSize);
  }

  public void insert(Table table, List<Row> rows, int chunkSize) {
    insert(table, rows, chunkSize, XOptional.empty());
  }

  private void insert(Table table, List<Row> rows, int chunkSize, XOptional<ReplaceOptions> replaceOptions) {
    new RowInserter().insert(this, table, rows, chunkSize, replaceOptions);
  }

  public void truncate(String tableName) {
    update("TRUNCATE table `" + tableName + "`");
  }

  public int delete(String query, Object... args) {
    return update(query, args);
  }

  public int update(String query, Object... args) {
    return new RowUpdater().update(this, query, args);
  }

  public void update(String table, Row row) {
    update(table, ImmutableList.of(row));
  }

  public void update(String table, Collection<Row> rows) {
    new RowUpdater().update(this, table, rows);
  }

  public long getCount(String countQuery, Object... args) {
    Row row = selectSingleRow(countQuery, args);
    Number n = (Number) first(row.map.values());
    return n.longValue();
  }

  public XSet<String> getSchemas() {
    log("getSchemas()");

    XSet<String> ret = XSet.create();
    Connection c = getConnection();
    try {
      ResultSet rs = c.getMetaData().getCatalogs();

      while (rs.next()) {
        ret.add(rs.getString("TABLE_CAT").toLowerCase());
      }
    } catch (Exception e) {
      throw propagate(e);
    } finally {
      close(c);
    }

    return ret;
  }


  public abstract boolean hasTable(String table);

  public abstract boolean hasColumn(String table, String column);

  public boolean hasForeignKey(String sourceTable, String sourceColumn, String foreignTable, String foreignColumn) {
    return null != selectSingleRow("SELECT `COLUMN_NAME`"
        + " FROM INFORMATION_SCEHMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND REFERENCED_TABLE_NAME = ? AND REFERENCED_COLUMN_NAME = ? LIMIT 1",
        sourceTable, sourceColumn, foreignTable, foreignColumn);
  }

  public String getForeignKeyName(String sourceTable, String sourceColumn, String foreignTable, String foreignColumn) {
    return selectSingleRow("SELECT `CONSTRAINT_NAME`"
        + " FROM INFORMATION_SCEHMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND REFERENCED_TABLE_NAME = ? AND REFERENCED_COLUMN_NAME = ? LIMIT 1",
        sourceTable, sourceColumn, foreignTable, foreignColumn).get("CONSTRAINT_NAME");
  }

  public XSet<String> getTables() {
    return getTables(false);
  }

  public XSet<String> getTables(boolean lowercase) {
    log("getTables()");

    XSet<String> ret = XSet.create();
    Connection c = getConnection();
    try {
      ResultSet rs = c.getMetaData().getTables(schema, schema, "%", new String[] { "TABLE" });

      while (rs.next()) {
        String s = rs.getString(3);
        if (lowercase) {
          s = s.toLowerCase();
        }
        ret.add(s);
      }
    } catch (Exception e) {
      throw propagate(e);
    } finally {
      close(c);
    }
    return ret;
  }

  public XSet<String> getTablesWithColumn(String columnName) {
    XList<String> ret = selectSingleColumn("SELECT DISTINCT TABLE_NAME FROM information_schema.columns"
        + " WHERE COLUMN_NAME = ? AND TABLE_SCHEMA = ?", columnName, schema);
    return ret.toSet();
  }

  /**
   * Returns true if the table was added, false if the table already exists.
   */
  public boolean addTable(Table table) {
    checkNotNull(table);

    table.databaseType(databaseType);

    if (getTables(true).contains(table.name)) {
      return false;
    }

    execute(table.toSQL(schema));
    for (Index index : table.indices) {
      addIndex(table.name, index.columns, index.unique);
    }

    return true;
  }

  public DB wipe() {
    deleteTables(getTables().toList());
    return this;
  }

  public DB clearAllRows() {
    for (String table : getTables()) {
      clearRows(table);
    }
    return this;
  }

  public void clearRows(String table) {
    execute("DELETE FROM `" + schema + "`.`" + table + "`");
  }

  public void addColumn(String table, String column, Class<?> columnType) {
    addColumn(table, column, Table.getType(databaseType, columnType));
  }

  public void addColumn(String table, String column, String columnType) {
    String s = "ALTER TABLE `" + table + "` ADD `" + column + "` " + columnType;
    execute(s);
  }

  public Map<String, String> getColumns(String table) {
    List<Row> rows = select("SELECT `COLUMN_NAME` as `name`, `DATA_TYPE` as `type`,"
        + " `CHARACTER_MAXIMUM_LENGTH` as `len`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?",
        schema, table);

    Map<String, String> ret = Maps.newLinkedHashMap();
    for (Row row : rows) {
      Object o = row.getObject("type");
      String type = o instanceof String ? (String) o : new String((byte[]) o);
      if (type.equals("varchar") || type.equals("char")) {
        type += "(" + row.getObject("len") + ")";
      }

      ret.put(row.get("name"), type);
    }
    return ret;
  }

  public void addIndex(String table, String column) {
    addIndex(table, column, false);
  }

  public void addIndex(String table, String column, boolean unique) {
    addIndex(table, ImmutableList.of(column), unique);
  }

  public void addIndex(String table, Collection<String> columns, boolean unique) {
    addIndex(table, columns, unique, Joiner.on("_").join(columns));
  }

  protected abstract void addIndex(String table, Collection<String> columns, boolean unique, String indexName);

  /**
   * Whether the table has an index of the given name. Only recommended use is single-column indices, in which case the
   * index name will be the column name.
   */
  public boolean hasIndex(String tableName, String indexName) {
    return select("SHOW INDEX FROM `" + tableName + "`")
        .toSet(r -> r.get("Key_name"))
        .contains(indexName);
  }

  public void removeIndex(String table, String indexName) {
    execute("ALTER TABLE `" + table + "` DROP INDEX `" + indexName + "`");
  }

  public void deleteTable(String table) {
    execute("DROP TABLE " + escape(schema) + "." + escape(table));
  }

  public void deleteTables(XList<String> tables) {
    execute("DROP TABLE " + Joiner.on(", ").join(tables.map(table -> "`" + schema + "`.`" + table + "`")));
  }

  public void renameTable(String oldName, String newName) {
    execute("RENAME TABLE `" + oldName + "` TO `" + newName + "`");
  }

  public void renameColumn(String table, String oldName, String newName) {
    Row row = selectSingleRow("SELECT COLUMN_TYPE"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
        schema, table, oldName);

    String type = row.get("COLUMN_TYPE");

    execute("ALTER TABLE `" + schema + "`.`" + table + "` CHANGE `" + oldName + "` `" + newName + "` " + type);
  }

  public void changeColumnType(String table, String columnName, String columnType) {
    checkNotEmpty(table);
    checkNotEmpty(columnName);
    checkNotEmpty(columnType);

    execute("ALTER TABLE `" + schema + "`.`" + table +
        "` CHANGE `" + columnName + "` `" + columnName + "` " + columnType);
  }

  public void changeColumnCollation(String table, String columnName, String collation) {
    Row row = selectSingleRow("SELECT COLUMN_TYPE"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
        schema, table, columnName);
    String type = row.get("COLUMN_TYPE");
    execute("ALTER TABLE `" + schema + "`.`" + table +
        "` CHANGE `" + columnName + "` `" + columnName + "` " + type + " COLLATE " + collation);
  }

  public void resetAutoIncrement(String table) {
    Row row = selectSingleRow("SELECT id FROM `" + table + "` ORDER BY id DESC LIMIT 1");
    long maxId = row == null ? 0 : row.getId();

    execute("ALTER TABLE `" + schema + "`.`" + table + "` AUTO_INCREMENT = " + (maxId + 1));
  }

  public void deleteColumn(String table, String column) {
    execute("ALTER TABLE `" + table + "` DROP COLUMN `" + column + "`");
  }

  public void deleteColumns(String table, Iterable<String> columns) {
    columns.forEach(column -> deleteColumn(table, column));
  }

  public void removeForeignKey(String table, String foreignKeyName) {
    execute("ALTER TABLE `" + table + "` DROP FOREIGN KEY " + foreignKeyName);
  }

  public void removeForeignKey(String sourceTable, String sourceColumn, String foreignTable, String foreignColumn) {
    execute("ALTER TABLE `" + sourceTable + "` DROP FOREIGN KEY "
        + getForeignKeyName(sourceTable, sourceColumn, foreignTable, foreignColumn));
  }

  /**
   * @return Number of bytes of disk usage (not including indices)
   */
  public long getDiskUsage(String table) {
    // index_length gets the space that the indices take up
    Row row = selectSingleRow(
        "SELECT data_length FROM information_schema.tables WHERE table_schema = ? and table_name = ?");
    return row.getLong("data_length");
  }

  public void execute(String statement) {
    log(statement);

    Connection c = getConnection();
    try {
      Statement s = c.createStatement();
      s.executeUpdate(statement);
      s.close();
    } catch (Exception e) {
      System.err.println("Problem executing statement:\n " + statement);
      throw propagate(e);
    } finally {
      close(c);
    }
  }

  protected void close(Connection c) {
    if (transactionConnections.get() != null) {
      // we're in a transaction, so don't close the connection.
      return;
    }
    try {
      if (normalize(disableForeignKeyChecks.get())) {
        try {
          Statement s = c.createStatement();
          s.executeUpdate("SET FOREIGN_KEY_CHECKS = 1");
          close(s);
        } catch (Exception e) {
          throw propagate(e);
        }
      }
      c.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void close(Statement statement) {
    if (statement == null) {
      return;
    }
    try {
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void close(ResultSet results) {
    if (results != null) {
      try {
        results.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public Connection getConnection() {
    Connection ret = transactionConnections.get();
    if (ret == null) {
      try {
        ret = source.getConnection();
      } catch (Exception e) {
        throw propagate(e);
      }
    }
    if (normalize(disableForeignKeyChecks.get())) {
      try {
        Statement s = ret.createStatement();
        s.executeUpdate("SET FOREIGN_KEY_CHECKS = 0");
        close(s);
      } catch (Exception e) {
        throw propagate(e);
      }
    }
    return ret;
  }

  /**
   * Closes all connections to this database. Future queries using this DB object will fail.
   */
  public void shutdown() {
    source.close();
  }

  private static final Set<Class<?>> whitelist = Sets.newHashSet(Number.class, String.class, Boolean.class);

  public static Object convert(Object o) {
    if (o instanceof Optional) {
      o = ((Optional<?>) o).orElse(null);
    } else if (o instanceof XOptional) {
      o = ((XOptional<?>) o).orElseNull();
    }
    if (o == null) {
      return o;
    }
    if (o instanceof UUID) {
      return o.toString();
    } else if (o instanceof LocalDateTime) {
      return o.toString();
    } else if (o instanceof LocalDate) {
      return java.sql.Date.valueOf((LocalDate) o);
    } else if (o instanceof Instant) {
      return ((Instant) o).toEpochMilli();
    } else if (o.getClass().isEnum()) {
      Enum<?> e = (Enum<?>) o;
      return e.name();
    } else if (o instanceof Json) {
      return o.toString();
    } else if (o instanceof Money) {
      return ((Money) o).toLong();
    } else if (o instanceof Percent) {
      return ((Percent) o).formatWithMaxLength(20);
    } else if (o instanceof ZoneId) {
      return o.toString();
    } else if (o.getClass().isArray()) {
      return o;
    } else {
      for (Class<?> c : whitelist) {
        if (c.isInstance(o)) {
          return o;
        }
      }
      return o.toString();
    }
  }

  void log(String query, Object... args) {
    log(query, null, args);
  }

  private void log(String query, Stopwatch watch, Object... args) {
    DebuggingData data = threadDebuggingData.get();
    if (data != null) {
      data.store(query, XList.of(args), watch == null ? null : watch.elapsed());
    }

    if (debug) {
      if (args.length > 0) {
        query += " [" + Arrays.toString(args) + "]";
      }
      if (watch != null) {
        query += " (" + watch + ")";
      }
      query = abbreviate(query, maxDebugLength);
      Log.debug(query);
    }
  }

  /**
   * For debugging
   */
  public void printPoolStats(String prefix) {
    HikariPool pool = Reflection.get(source, "pool");
    Method method = Reflection.getMethod(pool.getClass(), "logPoolState");
    try {
      method.invoke(pool, new Object[] { new String[] { prefix } });
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      e.printStackTrace();
    }
  }

  /**
   * Tracks all database requests performed during the callback and then reports debugging information about them.
   */
  public void captureDebuggingData(Runnable callback) {
    DebuggingData data = new DebuggingData();
    threadDebuggingData.set(data);
    try {
      callback.run();
      data.print();
    } finally {
      threadDebuggingData.set(null);
    }
  }

}
