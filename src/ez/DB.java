package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static ox.util.Functions.map;
import static ox.util.Utils.abbreviate;
import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.first;
import static ox.util.Utils.format;
import static ox.util.Utils.isNullOrEmpty;
import static ox.util.Utils.normalize;
import static ox.util.Utils.only;
import static ox.util.Utils.propagate;
import static ox.util.Utils.propagateInterruption;
import static ox.util.Utils.sleep;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.postgresql.util.PGobject;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;

import ez.RowInserter.ReplaceOptions;
import ez.Table.Index;
import ez.helper.DebuggingData;
import ez.helper.ForeignKeyConstraint;
import ez.misc.DatabaseType;
import ez.misc.IsolationLevel;
import ez.misc.RollbackException;

import ox.Json;
import ox.Log;
import ox.Money;
import ox.Percent;
import ox.Reflection;
import ox.x.XList;
import ox.x.XMap;
import ox.x.XOptional;
import ox.x.XSet;

public abstract class DB {

  /**
   * Used for testing. Simulates latency with the database.
   */
  public static int FAKE_LATENCY_MS = 0;

  public static Function<String, String> rootDriver = s -> "jdbc:" + s;
  public static boolean debug = false;
  public static int maxDebugLength = 2000;

  /**
   * Used to indicate that a value should be inserted as 'null' when calling insertRawRows()
   */
  public static final String NULL = "ez.DB.NULL";
  public final String primaryHost, primaryUser, primaryPass;
  private HikariDataSource primarySource, readOnlySource;

  /**
   * Making this Inheritable causes all manner of bugs (e.g. "java.sql.SQLException: Connection is closed"). Don't do
   * it!
   */
  protected final ThreadLocal<Connection> transactionConnections = new ThreadLocal<>();

  private final InheritableThreadLocal<DebuggingData> threadDebuggingData = new InheritableThreadLocal<>();
  protected final InheritableThreadLocal<Boolean> disableForeignKeyChecks = new InheritableThreadLocal<>();

  public final DatabaseType databaseType;
  public final String catalog;
  protected final String schema;
  public final boolean ssl;

  private static XOptional<Supplier<String>> traceIdSupplier = XOptional.empty();

  protected final int maxConnections;

  private boolean batchStatements = false;
  private List<String> batchedStatements = Collections.synchronizedList(new ArrayList<>());

  @SuppressWarnings("unused")
  private boolean checkForInterrupts = true;

  protected DB(DatabaseType databaseType, String schema) {
    this.databaseType = databaseType;
    primaryHost = primaryUser = primaryPass = "";
    if (databaseType == DatabaseType.POSTGRES) {
      this.catalog = schema;
      this.schema = "public";
    } else {
      this.catalog = "";
      this.schema = schema;
    }
    ssl = false;
    primarySource = null;
    this.maxConnections = 10;
  }

  public DB(DatabaseType databaseType, String host, String user, String pass, String schema, boolean ssl,
      int maxConnections) {
    this(databaseType, host, user, pass, "", schema, ssl, maxConnections);
  }

  public DB(DatabaseType databaseType, String host, String user, String pass, String catalog, String schema,
      boolean ssl, int maxConnections) {
    this.databaseType = checkNotNull(databaseType);
    primaryHost = host;
    primaryUser = user;
    primaryPass = pass;

    schema = normalize(schema);
    if (databaseType == DatabaseType.POSTGRES) {
      if (catalog.isEmpty()) {
        this.catalog = schema;
        this.schema = "public";
      } else {
        this.catalog = catalog;
        this.schema = schema;
      }
    } else {
      this.catalog = "";
      this.schema = schema;
    }

    this.ssl = ssl;
    this.maxConnections = maxConnections;

    try {
      DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
    } catch (SQLException e) {
      throw propagate(e);
    }

    try {
      DriverManager.registerDriver(new org.postgresql.Driver());
    } catch (SQLException e) {
      throw propagate(e);
    }

    primarySource = createSource(host, user, pass);
  }

  public void setReadOnlyReplica(String host, String user, String password) {
    readOnlySource = createSource(host, user, password);
  }

  private HikariDataSource createSource(String host, String user, String password) {
    String type = databaseType == DatabaseType.POSTGRES ? "postgresql" : "mysql";
    int port = databaseType == DatabaseType.POSTGRES ? 5432 : 3306;

    String url = format("{0}://{1}:{2}/{3}", type, host, port,
        (databaseType == DatabaseType.POSTGRES) ? catalog : schema);

    if (databaseType == DatabaseType.MYSQL) {
      if (ssl) {
        url += "?requireSSL=true&useSSL=true&verifyServerCertificate=true";
      } else {
        url += "?useSSL=false";
      }
      // url += "&useLegacyDatetimeCode=false";
      // url += "&serverTimezone=UTC";
      url += "&characterEncoding=utf8";
      url += "&zeroDateTimeBehavior=convertToNull";
    } else if (databaseType == DatabaseType.POSTGRES) {
      url += "?currentSchema=" + schema;
      // url += "?adaptiveFetch=true&defaultRowFetchSize=64&maxResultBuffer=128M";
    }

    url = rootDriver.apply(url);
    if (debug) {
      Log.debug(url);
    }

    HikariDataSource source = new HikariDataSource();
    source.setJdbcUrl(url);
    source.setUsername(user);
    source.setPassword(password);
    source.setMaximumPoolSize(maxConnections);
    // source.setLeakDetectionThreshold(2000);
    // source.setConnectionInitSql("SET NAMES utf8mb4");
    source.setAutoCommit(true);
    return source;
  }

  public String getSchema() {
    return schema;
  }

  public DB checkForInterrupts(boolean checkForInterrupts) {
    this.checkForInterrupts = checkForInterrupts;
    return this;
  }

  public void resetConnectionPool() {
    resetConnectionPool(primarySource);
    if (readOnlySource != null) {
      resetConnectionPool(readOnlySource);
    }
  }

  private void resetConnectionPool(HikariDataSource source) {
    HikariDataSource oldSource = source;
    HikariDataSource newSource = copySource(source);

    source = newSource;
    oldSource.close();
  }

  private HikariDataSource copySource(HikariDataSource source) {
    HikariDataSource ret = new HikariDataSource();
    ret.setJdbcUrl(source.getJdbcUrl());
    ret.setUsername(source.getUsername());
    ret.setPassword(source.getPassword());
    ret.setMaximumPoolSize(source.getMaximumPoolSize());
    ret.setAutoCommit(true);

    return ret;
  }

  public void batchStatements(Runnable callback) {
    checkState(batchStatements == false);

    batchStatements = true;
    callback.run();
    batchStatements = false;

    XList<String> toRun = XList.create(batchedStatements);
    batchedStatements.clear();
    execute(toRun);
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

    Connection conn = getConnection(true);
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
        Log.error(e);
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

  public boolean isInTransaction() {
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

  public XList<Row> select(String query, Collection<Object> args) {
    return select(query, args.toArray());
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
    query = appendTraceId(query);
    new RowSelector().select(this, query, r -> {
      try {
        ret.add((T) r.getObject(1));
      } catch (SQLException e) {
        throw propagate(e);
      }
    }, XOptional.empty(), args);
    return ret;
  }

  public void stream(String query, Consumer<Row> callback, Object... args) {
    stream(query, 32, callback, args);
  }

  public void stream(String query, int fetchSize, Consumer<Row> callback, Object... args) {
    stream(query, XOptional.of(fetchSize), true, callback, args);
  }

  public void stream(String query, XOptional<Integer> fetchSize, boolean reuseRows, Consumer<Row> callback,
      Object... args) {
    query = appendTraceId(query);
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

  public void insertRawRows(String table, List<? extends Iterable<?>> rows, boolean replace) {
    new RowInserter().insertRawRows(this, table, rows, replace);
  }

  public void replace(Table table, Row row) {
    replace(table, ImmutableList.of(row));
  }

  /**
   * REPLACE works exactly like INSERT, except that the old row is deleted before the new row is inserted (based on
   * primary key or unique index)
   */
  public void replace(Table table, List<Row> rows) {
    replace(table, rows, new ReplaceOptions("", XSet.create(), false));
  }

  public int replace(Table table, List<Row> rows, ReplaceOptions replaceOptions) {
    return insert(table, rows, 16_000, XOptional.of(replaceOptions));
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

  private int insert(Table table, List<Row> rows, int chunkSize, XOptional<ReplaceOptions> replaceOptions) {
    return new RowInserter().insert(this, table, rows, chunkSize, replaceOptions);
  }

  public void truncate(String tableName) {
    execute("TRUNCATE table " + escape(getSchema()) + "." + escape(tableName));
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
    update(table, rows, Collections.emptySet());
  }

  public void update(String table, Collection<Row> rows, Set<String> columnsToUpdate) {
    new RowUpdater().update(this, table, rows, columnsToUpdate);
  }

  public long getCount(String countQuery, Object... args) {
    Row row = selectSingleRow(countQuery, args);
    Number n = (Number) first(row.map.values());
    return n.longValue();
  }

  public String appendTraceId(String query) {
    String traceId = traceIdSupplier.compute(s -> s.get(), "");
    return isNullOrEmpty(traceId) ? query : "/*" + traceId + "*/ " + query;
  }

  public XSet<String> getSchemas() {
    log("getSchemas()");

    XSet<String> ret = XSet.create();
    Connection c = getConnection(true);
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

  public abstract boolean hasSchema(String schema);

  public abstract boolean hasTable(String table);

  public abstract boolean hasColumn(String table, String column);

  public void addForeignKey(ForeignKeyConstraint constraint) {
    constraint.execute(this);
  }

  public boolean hasForeignKey(String sourceTable, String sourceColumn, String foreignTable,
      String foreignColumn) {
    return !getForeignKeyName(sourceTable, sourceColumn, foreignTable, foreignColumn).isEmpty();
  }

  public abstract String getForeignKeyName(String sourceTable, String sourceColumn, String foreignTable,
      String foreignColumn);

  public XSet<String> getTables() {
    return getTables(false);
  }

  public XSet<String> getTables(boolean lowercase) {
    return this.<String>selectSingleColumn("SELECT TABLE_NAME FROM information_schema.tables"
        + " WHERE TABLE_SCHEMA = ? AND TABLE_TYPE != 'VIEW'", getSchema()).toSet();
  }

  public XSet<String> getTablesWithColumn(String columnName) {
    XList<String> ret = selectSingleColumn("SELECT DISTINCT c.TABLE_NAME \n"
        + "FROM information_schema.columns c\n"
        + "JOIN information_schema.tables t ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA\n"
        + "WHERE c.COLUMN_NAME = ? AND c.TABLE_SCHEMA = ? AND t.TABLE_TYPE != 'VIEW'", columnName, getSchema());
    return ret.toSet();
  }

  public String getCreateTableStatement(String tableName) {
    Row row = selectSingleRow("SHOW CREATE TABLE " + escape(tableName));
    String ret = row.get("Create Table");
    return ret.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ");
  }

  /**
   * Returns true if the table was added, false if the table already exists.
   */
  public boolean addTable(Table table) {
    checkNotNull(table);

    table.databaseType(databaseType);

    if (hasTable(table.name)) {
      return false;
    }

    execute(table.toSQL(getSchema()));
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
    execute(map(getTables(), table -> "TRUNCATE TABLE `" + getSchema() + "`.`" + table + "`"));
    return this;
  }

  public void clearRows(String table) {
    execute("DELETE FROM `" + getSchema() + "`.`" + table + "`");
  }

  public void addColumn(String table, String column, Class<?> columnType) {
    addColumn(table, column, Table.getType(databaseType, columnType));
  }

  public void addColumn(String table, String column, String columnType) {
    String s = "ALTER TABLE `" + table + "` ADD `" + column + "` " + columnType;
    execute(s);
  }

  /**
   * @return map from column name to type.
   */
  public XMap<String, String> getColumns(String table) {
    List<Row> rows = select("SELECT `COLUMN_NAME` as `name`, `DATA_TYPE` as `type`,"
        + " `CHARACTER_MAXIMUM_LENGTH` as `len`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?",
        getSchema(), table);

    XMap<String, String> ret = XMap.create();
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
    addIndex(table, XList.of(column), unique);
  }

  public void addIndex(String table, XList<String> columns, boolean unique) {
    String indexName = Joiner.on("_").join(columns);
    if (indexName.length() > 64) {
      int maxKeyLength = (64 - columns.size() - 1) / columns.size();
      checkState(maxKeyLength > 0);
      indexName = Joiner.on("_").join(columns.map(c -> c.length() > maxKeyLength ? c.substring(0, maxKeyLength) : c));
    }
    addIndex(table, columns, unique, indexName);
  }

  public abstract void addIndex(String table, XList<String> columns, boolean unique, String indexName);

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
    execute("DROP TABLE " + escape(getSchema()) + "." + escape(table));
  }

  public void deleteTables(XList<String> tables) {
    if (tables.isEmpty()) {
      return;
    }
    execute("DROP TABLE " + Joiner.on(", ").join(tables.map(table -> "`" + getSchema() + "`.`" + table + "`")));
  }

  public void renameTable(String oldName, String newName) {
    execute("RENAME TABLE `" + oldName + "` TO `" + newName + "`");
  }

  public void renameColumn(String table, String oldName, String newName) {
    Row row = selectSingleRow("SELECT COLUMN_TYPE"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
        getSchema(), table, oldName);

    if (row == null) {
      throw new RuntimeException("Column doesn't exist: " + table + "." + oldName);
    }

    String type = row.get("COLUMN_TYPE");

    execute("ALTER TABLE `" + getSchema() + "`.`" + table + "` CHANGE `" + oldName + "` `" + newName + "` " + type);
  }

  public void renameIndex(String table, String oldIndexName, String newIndexName) {
    execute("ALTER TABLE `" + getSchema() + "`.`" + table + "` RENAME INDEX " + oldIndexName + " " + newIndexName);
  }

  public void changeColumnType(String table, String columnName, String columnType) {
    checkNotEmpty(table);
    checkNotEmpty(columnName);
    checkNotEmpty(columnType);

    execute("ALTER TABLE `" + getSchema() + "`.`" + table +
        "` CHANGE `" + columnName + "` `" + columnName + "` " + columnType);
  }

  public void changeColumnCollation(String table, String columnName, String collation) {
    Row row = selectSingleRow("SELECT COLUMN_TYPE"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
        getSchema(), table, columnName);
    String type = row.get("COLUMN_TYPE");
    execute("ALTER TABLE `" + getSchema() + "`.`" + table +
        "` CHANGE `" + columnName + "` `" + columnName + "` " + type + " COLLATE " + collation);
  }

  public void resetAutoIncrement(String table) {
    Row row = selectSingleRow("SELECT id FROM `" + table + "` ORDER BY id DESC LIMIT 1");
    long maxId = row == null ? 0 : row.getId();

    execute("ALTER TABLE `" + getSchema() + "`.`" + table + "` AUTO_INCREMENT = " + (maxId + 1));
  }

  public void deleteColumn(String table, String column) {
    execute("ALTER TABLE `" + table + "` DROP COLUMN `" + column + "`");
  }

  public void deleteColumns(String table, Iterable<String> columns) {
    columns.forEach(column -> deleteColumn(table, column));
  }

  public void removeForeignKey(String table, String foreignKeyName) {
    execute("ALTER TABLE " + databaseType.escape(table) + " DROP CONSTRAINT " + foreignKeyName);
  }

  public void removeForeignKey(String sourceTable, String sourceColumn, String foreignTable, String foreignColumn) {
    String foreignKeyName = getForeignKeyName(sourceTable, sourceColumn, foreignTable, foreignColumn);
    if (!foreignKeyName.isEmpty()) {
      removeForeignKey(sourceTable, foreignKeyName);
    }
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

  public void execute(XList<String> statements) {
    statements.forEach(this::log);

    Connection c = getConnection(true);
    try {
      Statement s = c.createStatement();
      statements.forEach(statement -> {
        try {
          s.addBatch(statement);
        } catch (SQLException e) {
          throw propagate(e);
        }
      });
      s.executeBatch();
      s.close();
    } catch (Exception e) {
      System.err.println("Problem executing statements:\n " + Joiner.on('\n').join(statements));
      throw propagate(e);
    } finally {
      close(c);
    }
  }

  public void execute(String statement) {
    if (batchStatements) {
      batchedStatements.add(statement);
      return;
    }

    log(statement);

    Connection c = getConnection(true);
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
        enableReferentialConstraints(c);
      }
      c.close();
    } catch (Exception e) {
      Log.error(e);
    }
  }

  protected void close(Statement statement) {
    if (statement == null) {
      return;
    }
    try {
      statement.close();
    } catch (Exception e) {
      Log.error(e);
    }
  }

  protected void disableReferentialConstraints(Connection c) {
    try {
      Statement s = c.createStatement();
      s.executeUpdate("SET FOREIGN_KEY_CHECKS = 0");
      close(s);
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  protected void enableReferentialConstraints(Connection c) {
    try {
      Statement s = c.createStatement();
      s.executeUpdate("SET FOREIGN_KEY_CHECKS = 1");
      close(s);
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  void close(ResultSet results) {
    if (results != null) {
      try {
        results.close();
      } catch (Exception e) {
        Log.error(e);
      }
    }
  }

  public Connection getConnection() {
    return getConnection(false);
  }

  public Connection getConnection(boolean forcePrimary) {
    // if (checkForInterrupts) {
    // Thread thread = Thread.currentThread();
    // Log.debug(thread + " :: " + thread.isInterrupted());
    // if (thread.isInterrupted()) {
    // throw new ThreadInterruptedException();
    // }
    // }
    if (FAKE_LATENCY_MS > 0) {
      sleep(FAKE_LATENCY_MS);
    }

    Connection ret = transactionConnections.get();
    if (ret == null) {
      HikariDataSource source = primarySource;
      try {
        if (readOnlySource != null && !forcePrimary) {
          source = readOnlySource;
          ret = readOnlySource.getConnection();
          ret.setReadOnly(true);
        } else {
          ret = primarySource.getConnection();
        }
      } catch (Exception e) {
        propagateInterruption(e);
        throw new RuntimeException("Problem connecting to " + source.getJdbcUrl(), e);
      }
    }
    if (normalize(disableForeignKeyChecks.get())) {
      disableReferentialConstraints(ret);
    }
    return ret;
  }

  /**
   * Closes all connections to this database. Future queries using this DB object will fail.
   */
  public void shutdown() {
    primarySource.close();
  }

  public static void traceIdSupplier(Supplier<String> traceIdSupplier) {
    DB.traceIdSupplier = XOptional.of(traceIdSupplier);
  }

  private static final Set<Class<?>> whitelist = Sets.newHashSet(Number.class, String.class, Boolean.class,
      PGobject.class);

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

  void log(String query, Stopwatch watch, Object... args) {
    DebuggingData data = threadDebuggingData.get();
    if (data != null) {
      data.store(query, XList.of(args), watch == null ? null : watch.elapsed());
    }

    if (debug) {
      if (args.length > 0) {
        long nQuestionMarks = query.chars().filter(i -> i == '?').count();
        if (nQuestionMarks == args.length) {
          StringBuilder sb = new StringBuilder();
          int argIndex = 0;
          for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '?') {
              Object o = args[argIndex++];
              if (o instanceof LocalDate || o instanceof String) {
                sb.append("'").append(o).append("'");
              } else {
                sb.append(o);
              }
            } else {
              sb.append(c);
            }
          }
          query = sb.toString();
        } else {
          query += " [" + Arrays.toString(args) + "]";
        }
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
    HikariPool pool = Reflection.get(primarySource, "pool");
    Method method = Reflection.getMethod(pool.getClass(), "logPoolState");
    try {
      method.invoke(pool, new Object[] { new String[] { prefix } });
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Log.error(e);
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

  public HikariDataSource getConnectionPool() {
    return primarySource;
  }

}
