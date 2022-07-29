package ez;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static ox.util.Functions.map;
import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.first;
import static ox.util.Utils.normalize;
import static ox.util.Utils.only;
import static ox.util.Utils.propagate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;

import ez.Table.Index;

import ox.Json;
import ox.Log;
import ox.Money;
import ox.Percent;
import ox.Reflection;
import ox.x.XList;
import ox.x.XOptional;
import ox.x.XSet;

public class DB {

  public static boolean debug = false;

  /**
   * Used to indicate that a value should be inserted as 'null' when calling insertRawRows()
   */
  public static final String NULL = "ez.DB.NULL";

  private final HikariDataSource source;
  protected final ThreadLocal<Connection> transactionConnections = new ThreadLocal<>();

  public final String host, user, pass;
  public final String schema;
  public final boolean ssl;

  private final int maxConnections;

  protected DB(String schema) {
    host = user = pass = "";
    this.schema = schema;
    ssl = false;
    source = null;
    this.maxConnections = 10;
  }

  public DB(String host, String user, String pass, String schema) {
    this(host, user, pass, schema, false);
  }

  public DB(String host, String user, String pass, String schema, boolean ssl) {
    this(host, user, pass, schema, ssl, 10);
  }

  public DB(String host, String user, String pass, String schema, boolean ssl, int maxConnections) {
    this.host = host;
    this.user = user;
    this.pass = pass;
    this.schema = normalize(schema);
    this.ssl = ssl;
    this.maxConnections = maxConnections;

    try {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver());
    } catch (SQLException e) {
      throw propagate(e);
    }

    String url = "jdbc:mysql://" + host + ":3306/" + schema;
    if (ssl) {
      url += "?requireSSL=true&useSSL=true&verifyServerCertificate=true";
    } else {
      url += "?useSSL=false";
    }
    // url += "&useLegacyDatetimeCode=false";
    // url += "&serverTimezone=UTC";
    url += "&characterEncoding=utf8";

    source = new HikariDataSource();
    source.setJdbcUrl(url);
    source.setUsername(user);
    source.setPassword(pass);
    source.setMaximumPoolSize(maxConnections);
    // source.setConnectionInitSql("SET NAMES utf8mb4");
    source.setAutoCommit(true);
  }

  /**
   * Creates the schema if it doesn't already exist.
   */
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
      DB temp = new DB(host, user, pass, "", ssl);
      temp.createSchema(schema);
      temp.shutdown();
    }
    return this;
  }

  public DB usingSchema(String schema) {
    schema = normalize(schema);
    if (!schema.isEmpty()) {
      checkArgument(isValidName(schema));
      if (!getSchemas().contains(schema.toLowerCase())) {
        createSchema(schema);
      }
    }
    return new DB(host, user, pass, schema, ssl, maxConnections);
  }

  /**
   * Imports a table into the current schema.
   */
  public DB importTable(String fromSchema, String tableName) {
    execute("RENAME TABLE `" + fromSchema + "`.`" + tableName + "` TO `" + tableName + "`");
    return this;
  }

  public void createSchema(String schema) {
    execute("CREATE DATABASE `" + schema + "` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_bin");
  }

  /**
   * Checks if this is a valid name for a table, column, etc. Must consist of only letters, numbers, and underscores.
   */
  public static boolean isValidName(String name) {
    return Pattern.matches("^[a-z][a-z0-9_.]*$", name);
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
    stream(query, row -> {
      ret.add(row);
    }, false, args);
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
    }, args);
    return ret;
  }

  private void select(String query, Consumer<ResultSet> rowCallback, Object... args) {
    Stopwatch watch = Stopwatch.createStarted();

    Connection conn = getConnection();
    PreparedStatement statement = null;
    ResultSet r = null;
    try {
      statement = conn.prepareStatement(query);

      for (int c = 0; c < args.length; c++) {
        statement.setObject(c + 1, convert(args[c]));
      }
      try {
        r = statement.executeQuery();
      } catch (Exception e) {
        System.err.println("Problem with query: " + query);
        throw propagate(e);
      }
      while (r.next()) {
        rowCallback.accept(r);
      }
    } catch (Exception e) {
      throw propagate(e);
    } finally {
      log(query, watch, args);
      close(r);
      close(statement);
      close(conn);
    }
  }

  public void stream(String query, Consumer<Row> callback, Object... args) {
    stream(query, callback, true, args);
  }

  public void stream(String query, Consumer<Row> callback, boolean reuseRows, Object... args) {
    Row row = new Row();
    List<String> labels = Lists.newArrayList();
    select(query, r -> {
      try {
        if (labels.isEmpty()) {
          ResultSetMetaData metadata = r.getMetaData();
          for (int i = 1; i <= metadata.getColumnCount(); i++) {
            labels.add(metadata.getColumnLabel(i));
          }
        }
        Row theRow = row;
        if (reuseRows) {
          theRow.map.clear();
        } else {
          theRow = new Row();
        }
        for (int i = 1; i <= labels.size(); i++) {
          Object val = r.getObject(i);
          if (val instanceof Clob) {
            Clob clob = (Clob) val;
            val = clob.getSubString(1, Math.toIntExact(clob.length()));
          }
          theRow.with(labels.get(i - 1), val);
        }
        callback.accept(theRow);
      } catch (SQLException e) {
        throw propagate(e);
      }
    }, args);
  }

  public void streamBulk(String query, Consumer<XList<Row>> callback, int chunkSize, Object... args) {
    int offset = 0;

    while (true) {
      XList<Row> rows = select(query + " LIMIT " + offset + ", " + chunkSize, args);
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
  public Long insert(String table, Row row) {
    insert(table, ImmutableList.of(row));
    Object o = row.getObject("id");
    if (o instanceof Long) {
      return (Long) o;
    }
    return null;
  }

  public void insertRawRows(String table, List<? extends Iterable<?>> rows) {
    if (rows.isEmpty()) {
      return;
    }

    Connection conn = getConnection();
    PreparedStatement statement = null;

    try {
      StringBuilder sb = new StringBuilder("INSERT INTO `" + schema + "`.`" + table + "` VALUES ");

      final String placeholders = getInsertPlaceholders(Iterables.size(rows.get(0)));
      for (int i = 0; i < rows.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(placeholders);
      }

      String s = sb.toString();
      log(s);

      statement = conn.prepareStatement(s, Statement.NO_GENERATED_KEYS);

      int c = 1;
      for (Iterable<? extends Object> row : rows) {
        for (Object o : row) {
          if (o == NULL) {
            statement.setObject(c++, null);
          } else {
            statement.setObject(c++, o);
          }
        }
      }
      statement.execute();

    } catch (Exception e) {
      throw propagate(e);
    } finally {
      close(statement);
      close(conn);
    }
  }

  public void replace(String table, Row row) {
    replace(table, ImmutableList.of(row));
  }

  /**
   * REPLACE works exactly like INSERT, except that the old row is deleted before the new row is inserted (based on
   * primary key or unique index)
   */
  public void replace(String table, List<Row> rows) {
    insert(table, rows, 16_000, true);
  }

  public void insert(String table, List<Row> rows) {
    insert(table, rows, 16_000);
  }

  public void insert(String table, List<Row> rows, int chunkSize) {
    insert(table, rows, chunkSize, false);
  }

  private void insert(String table, List<Row> rows, int chunkSize, boolean replace) {
    if (Iterables.isEmpty(rows)) {
      return;
    }

    // break the inserts into chunks
    if (rows.size() > chunkSize) {
      for (int i = 0; i < rows.size(); i += chunkSize) {
        List<Row> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
        Stopwatch watch = Stopwatch.createStarted();
        insert(table, chunk, chunkSize, replace);
        Log.info("Inserted " + chunk.size() + " rows into " + table + " (" + watch + ")");
      }
      return;
    }

    Connection conn = getConnection();
    PreparedStatement statement = null;
    ResultSet generatedKeys = null;

    try {
      Row firstRow = first(rows);
      StringBuilder sb = new StringBuilder(firstRow.getInsertStatementFirstPart(schema, table, replace));
      sb.append(" VALUES ");

      final String placeholders = getInsertPlaceholders(firstRow.map.size());
      for (int i = 0; i < rows.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(placeholders);
      }

      String s = sb.toString();
      log(s);

      statement = conn.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);

      int c = 1;
      for (Row row : rows) {
        for (Object o : row.map.values()) {
          Object converted = convert(o);
          statement.setObject(c++, converted);
        }
      }
      statement.execute();
      generatedKeys = statement.getGeneratedKeys();

      Iterator<Row> iter = rows.iterator();
      while (generatedKeys.next() && iter.hasNext()) {
        Object key = generatedKeys.getObject(1);
        if (key instanceof Long) {
          iter.next().with("id", key);
        } else if (key instanceof Number) {
          iter.next().with("id", ((Number) key).longValue());
        } else {
          iter.next().with("id", key);
        }
      }

    } catch (Exception e) {
      throw propagate(e);
    } finally {
      close(generatedKeys);
      close(statement);
      close(conn);
    }

  }

  private String getInsertPlaceholders(int placeholderCount) {
    final StringBuilder builder = new StringBuilder("(");
    for (int i = 0; i < placeholderCount; i++) {
      if (i != 0) {
        builder.append(",");
      }
      builder.append("?");
    }
    return builder.append(")").toString();
  }

  public void truncate(String tableName) {
    update("TRUNCATE table `" + tableName + "`");
  }

  public int delete(String query, Object... args) {
    return update(query, args);
  }

  public int update(String query, Object... args) {
    log(query);

    Connection conn = getConnection();
    PreparedStatement statement = null;
    try {
      statement = conn.prepareStatement(query);
      int c = 1;
      for (Object arg : args) {
        statement.setObject(c++, convert(arg));
      }
      return statement.executeUpdate();
    } catch (Exception e) {
      System.err.println("query: " + query);
      throw propagate(e);
    } finally {
      close(statement);
      close(conn);
    }
  }

  public void update(String table, Row row) {
    update(table, ImmutableList.of(row));
  }

  public void update(String table, Collection<Row> rows) {
    if (rows.isEmpty()) {
      return;
    }

    Connection conn = getConnection();
    PreparedStatement statement = null;
    String query = "";
    try {
      query = getFirst(rows, null).getUpdateStatement(schema, table);
      log(query);

      statement = conn.prepareStatement(query);
      for (Row row : rows) {
        int c = 1;
        for (Entry<String, Object> e : row.map.entrySet()) {
          if (e.getKey().equals("id")) {
            continue;
          }
          statement.setObject(c++, convert(e.getValue()));
        }
        statement.setObject(c++, convert(row.map.get("id")));
        statement.addBatch();
      }
      statement.executeBatch();
    } catch (Exception e) {
      System.err.println("query: " + query);
      throw propagate(e);
    } finally {
      close(statement);
      close(conn);
    }
  }

  public long getCount(String countQuery, Object... args) {
    Row row = selectSingleRow(countQuery, args);
    Number n = (Number) first(row.map.values());
    return n.longValue();
  }

  public Set<String> getSchemas() {
    log("getSchemas()");

    Set<String> ret = Sets.newHashSet();
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

  public boolean hasTable(String table) {
    return null != selectSingleRow("SELECT `COLUMN_NAME`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? limit 1",
        schema, table);
  }

  public boolean hasColumn(String table, String column) {
    return null != selectSingleRow("SELECT `COLUMN_NAME`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND column_name = ? limit 1",
        schema, table, column);
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

  /**
   * Returns true if the table was added, false if the table already exists.
   */
  public boolean addTable(Table table) {
    checkNotNull(table);

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
    addColumn(table, column, Table.getType(columnType));
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

  protected void addIndex(String table, Collection<String> columns, boolean unique, String indexName) {
    List<String> cols = map(columns, s -> '`' + s + '`');
    String s = unique ? "ADD UNIQUE INDEX" : "ADD INDEX";
    execute("ALTER TABLE `" + table + "` " + s + " `" + indexName + "` (" + Joiner.on(",").join(cols) + ")");
  }

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
    execute("DROP TABLE `" + schema + "`.`" + table + "`");
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

  private void close(Connection c) {
    if (transactionConnections.get() != null) {
      // we're in a transaction, so don't close the connection.
      return;
    }
    try {
      c.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void close(Statement statement) {
    if (statement == null) {
      return;
    }
    try {
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void close(ResultSet results) {
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
    if (ret != null) {
      return ret;
    }
    try {
      // if (debug) {
      // printPoolStats("Before getConnection()");
      // Thread.dumpStack();
      // }
      return source.getConnection();
    } catch (Exception e) {
      throw propagate(e);
    } finally {
      // if (debug) {
      // printPoolStats("After getConnection()");
      // }
    }
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

  private void log(String query, Object... args) {
    log(query, null, args);
  }

  private void log(String query, Stopwatch watch, Object... args) {
    if (debug) {
      if (args.length > 0) {
        query += " [" + Arrays.toString(args) + "]";
      }
      if (watch != null) {
        query += " (" + watch + ")";
      }
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

  public static class ColumnBuilder {

    private String table, name, type, defaultValue, after;
    private boolean index = false, unique = false, caseSensitive = true, notNull = false;

    /**
     * Whether the column should be inserted as index 0.
     */
    private boolean first = false;

    private ColumnBuilder(String table) {
      this.table = table;
    }

    public ColumnBuilder name(String name) {
      this.name = name;
      return this;
    }

    public ColumnBuilder type(Class<?> type) {
      return type(Table.getType(type));
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
      StringBuilder sb = new StringBuilder("ALTER TABLE `");
      sb.append(table).append("` ADD `").append(name).append("` ").append(type);
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
        sb.append(" FIRST");
      } else if (after != null) {
        sb.append(" AFTER `").append(after).append('`');
      }

      db.execute(sb.toString());

      if (index) {
        db.addIndex(table, name, unique);
      }
    }

    public static ColumnBuilder create(String table, String name, Class<?> type) {
      return create(table, name, Table.getType(type));
    }

    public static ColumnBuilder create(String table, String name, String type) {
      return new ColumnBuilder(table).name(name).type(type);
    }

    public static ColumnBuilder table(String table) {
      return new ColumnBuilder(table);
    }

  }

  public static enum IsolationLevel {
    READ_UNCOMMITTED(1),
    READ_COMMITTED(2),
    REPEATABLE_READ(4),
    SERIALIZABLE(8);

    public final int level;

    private IsolationLevel(int level) {
      this.level = level;
    }
  }

  public static interface RollbackException {

    /**
     * Gets whether this Exception should trigger a rollback.
     */
    public default boolean shouldRollback() {
      return true;
    }

  }

}
