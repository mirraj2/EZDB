package ez;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static ox.util.Functions.map;
import static ox.util.Utils.first;
import static ox.util.Utils.only;
import static ox.util.Utils.propagate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;

import ez.Table.Index;
import ox.Json;
import ox.Log;
import ox.Money;

public class DB {

  public static final boolean debug = false;

  private final HikariDataSource source;
  private final ThreadLocal<Connection> transactionConnections = new ThreadLocal<>();

  public final String ip, user, pass;
  public final String schema;
  public final boolean ssl;

  public DB(String ip, String user, String pass, String schema) {
    this(ip, user, pass, schema, false);
  }

  public DB(String ip, String user, String pass, String schema, boolean ssl) {
    this.ip = ip;
    this.user = user;
    this.pass = pass;
    this.schema = schema;
    this.ssl = ssl;

    String url = "jdbc:mysql://" + ip + ":3306/" + schema + "?characterEncoding=utf8";
    if (ssl) {
      url += "&requireSSL=true&useSSL=true&verifyServerCertificate=true";
    }

    source = new HikariDataSource();
    source.setJdbcUrl(url);
    source.setUsername(user);
    source.setPassword(pass);
    source.setAutoCommit(true);
  }

  public DB usingSchema(String schema) {
    checkArgument(isValidName(schema));
    if (Strings.isNullOrEmpty(schema)) {
      schema = null;
    }

    if (!getSchemas().contains(schema.toLowerCase())) {
      execute("CREATE DATABASE `" + schema + "` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_bin");
    }

    return new DB(ip, user, pass, schema, ssl);
  }

  /**
   * Checks if this is a valid name for a table, column, etc. Must consist of only letters, numbers, and underscores.
   */
  public static boolean isValidName(String name) {
    return Pattern.matches("^[a-z][a-z0-9_.]*$", name);
  }

  public DB transaction(Runnable r) {
    Connection conn = getConnection();
    try {
      conn.setAutoCommit(false);
      transactionConnections.set(conn);
      r.run();
      conn.commit();
    } catch (Exception e) {
      try {
        conn.rollback();
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

  public List<Row> select(String query, Object... args) {
    List<Row> ret = Lists.newArrayList();
    stream(query, row -> {
      ret.add(row);
    }, false, args);
    return ret;
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> selectSingleColumn(String query, Object... args) {
    List<T> ret = Lists.newArrayList();
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
    log(query, args);

    Connection conn = getConnection();
    PreparedStatement statement = null;
    ResultSet r = null;
    try {
      statement = conn.prepareStatement(query);

      for (int c = 0; c < args.length; c++) {
        statement.setObject(c + 1, convert(args[c]));
      }
      r = statement.executeQuery();
      while (r.next()) {
        rowCallback.accept(r);
      }
    } catch (Exception e) {
      System.err.println("Problem with query: " + query);
      throw propagate(e);
    } finally {
      close(r);
      close(statement);
      close(conn);
    }
  }

  public void stream(String query, Consumer<Row> callback, Object... args) {
    stream(query, callback, true, args);
  }

  private void stream(String query, Consumer<Row> callback, boolean reuseRows, Object... args) {
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
          theRow.with(labels.get(i - 1), r.getObject(i));
        }
        callback.accept(theRow);
      } catch (SQLException e) {
        throw propagate(e);
      }
    }, args);
  }

  public void streamBulk(String query, Consumer<List<Row>> callback, int chunkSize, Object... args) {
    int offset = 0;

    while (true) {
      List<Row> rows = select(query + " LIMIT " + offset + ", " + chunkSize, args);
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

  public <T> void insertRawRows(String table, List<List<T>> rows) {
    if (rows.isEmpty()) {
      return;
    }

    Connection conn = getConnection();
    PreparedStatement statement = null;

    try {
      StringBuilder sb = new StringBuilder("INSERT INTO `" + schema + "`.`" + table + "` VALUES ");

      final String placeholders = getInsertPlaceholders(rows.get(0).size());
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
      for (List<? extends Object> row : rows) {
        for (Object o : row) {
          statement.setObject(c++, o);
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

  public void replace(String table, List<Row> rows) {
    insert(table, rows, true);
  }

  public void insert(String table, List<Row> rows) {
    insert(table, rows, false);
  }

  private void insert(String table, List<Row> rows, boolean replace) {
    if (Iterables.isEmpty(rows)) {
      return;
    }

    // break the inserts into chunks
    int chunkSize = 16000;
    if (rows.size() > chunkSize) {
      for (int i = 0; i < rows.size(); i += chunkSize) {
        List<Row> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
        Stopwatch watch = Stopwatch.createStarted();
        insert(table, chunk, replace);
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
          statement.setObject(c++, convert(o));
        }
        // statement.addBatch();
      }
      // statement.executeBatch();
      statement.execute();
      generatedKeys = statement.getGeneratedKeys();

      Iterator<Row> iter = rows.iterator();
      while (generatedKeys.next() && iter.hasNext()) {
        long id = generatedKeys.getLong(1);
        iter.next().with("id", id);
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
      // statement.executeUpdate();
    } catch (Exception e) {
      System.err.println("query: " + query);
      throw propagate(e);
    } finally {
      close(statement);
      close(conn);
    }
  }

  public int getCount(String countQuery, Object... args) {
    Row row = selectSingleRow(countQuery, args);
    Number n = (Number) first(row.map.values());
    return n.intValue();
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

  public Set<String> getTables() {
    return getTables(false);
  }

  public Set<String> getTables(boolean lowercase) {
    log("getTables()");

    Set<String> ret = Sets.newHashSet();
    Connection c = getConnection();
    try {
      ResultSet rs = c.getMetaData().getTables(schema, null, "%", null);

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

  public void addTable(Table table) {
    checkNotNull(table);

    if (getTables(true).contains(table.name)) {
      throw new IllegalArgumentException("Table already exists: " + table.name);
    }

    execute(table.toSQL(schema));
    for (Index index : table.indices) {
      String indexName = Joiner.on("_").join(index.columns);
      List<String> cols = map(index.columns, s -> '`' + s + '`');
      String s = index.unique ? "ADD UNIQUE INDEX" : "ADD INDEX";
      execute("ALTER TABLE `" + table.name + "` " + s + " `" + indexName + "` (" + Joiner.on(",").join(cols) + ")");
    }
  }

  public DB wipe() {
    for (String table : getTables()) {
      deleteTable(table);
    }
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
      String type = row.get("type");
      if (type.equals("varchar") || type.equals("char")) {
        type += "(" + row.getObject("len") + ")";
      }

      ret.put(row.get("name"), type);
    }
    return ret;
  }

  public void addIndex(String table, String column, boolean unique) {
    String s = "ALTER TABLE `" + table + "` ADD ";
    if (unique) {
      s += "UNIQUE ";
    } else {
      s += "INDEX ";
    }
    s += "(`" + column + "`)";
    execute(s);
  }

  public void deleteTable(String table) {
    execute("DROP TABLE `" + schema + "`.`" + table + "`");
  }

  public void renameTable(String oldName, String newName) {
    execute("RENAME TABLE `" + oldName + "` TO `" + newName + "`");
  }

  public void renameColumn(String table, String oldName, String newName) {
    Row row = selectSingleRow("SELECT DATA_TYPE as `type`, CHARACTER_MAXIMUM_LENGTH as `len`"
        + " FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
        schema, table, oldName);

    String type = row.get("type");

    if (type.equals("varchar")) {
      type += "(" + row.getObject("len") + ")";
    }

    execute("ALTER TABLE `" + schema + "`.`" + table + "` CHANGE `" + oldName + "` `" + newName + "` " + type);
  }

  public void deleteColumn(String table, String column) {
    execute("ALTER TABLE `" + table + "` DROP COLUMN `" + column + "`");
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
      return source.getConnection();
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  private final Set<Class<?>> whitelist = Sets.newHashSet(Number.class, String.class, Boolean.class);

  private Object convert(Object o) {
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
      return ((Money) o).toInt();
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
    if (debug) {
      if (args.length > 0) {
        query += " [" + Arrays.toString(args) + "]";
      }
      if (query.length() > 1000) {
        query = query.substring(0, 1000);
      }
      Log.debug(query);
    }
  }

  public static class ColumnBuilder {

    private String table, name, type, defaultValue, after;
    private boolean index = false;

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

    public ColumnBuilder index() {
      index = true;
      return this;
    }

    public void execute(DB db) {
      StringBuilder sb = new StringBuilder("ALTER TABLE `");
      sb.append(table).append("` ADD `").append(name).append("` ").append(type);
      if (defaultValue != null) {
        sb.append(" DEFAULT '").append(defaultValue).append("'");
      }
      if (after != null) {
        sb.append(" AFTER `").append(after).append('`');
      }
      db.execute(sb.toString());

      if (index) {
        db.addIndex(table, name, false);
      }
    }

    public static ColumnBuilder table(String table) {
      return new ColumnBuilder(table);
    }
  }

}
