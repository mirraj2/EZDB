package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getOnlyElement;
import static ox.util.Functions.map;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariDataSource;
import ox.Json;

public class DB {

  private final HikariDataSource source;
  private String schema = null;
  private final ThreadLocal<Connection> transactionConnections = new ThreadLocal<>();

  public DB(String ip, String user, String pass, String schema) {
    this.schema = schema;

    source = new HikariDataSource();
    source.setJdbcUrl("jdbc:mysql://" + ip + ":3306/" + schema);
    source.setUsername(user);
    source.setPassword(pass);
    source.setAutoCommit(true);
  }

  public DB usingSchema(String schema) {
    if (Strings.isNullOrEmpty(schema)) {
      schema = null;
    }

    if (!getSchemas().contains(schema.toLowerCase())) {
      execute("Create Database " + schema);
    }

    this.schema = schema;

    return this;
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
        throw Throwables.propagate(ee);
      }
      throw Throwables.propagate(e);
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
    return getOnlyElement(select(query, args), null);
  }

  public List<Row> select(String query, Object... args) {
    Connection conn = getConnection();
    PreparedStatement statement = null;
    ResultSet r = null;
    try {
      statement = conn.prepareStatement(query);

      for (int c = 0; c < args.length; c++) {
        statement.setObject(c + 1, convert(args[c]));
      }
      r = statement.executeQuery();
      List<Row> ret = Lists.newArrayList();
      ResultSetMetaData meta = r.getMetaData();
      while (r.next()) {
        Row row = new Row();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          row.with(meta.getColumnLabel(i), r.getObject(i));
        }
        ret.add(row);
      }
      return ret;
    } catch (Exception e) {
      System.err.println("Problem with query: " + query);
      throw Throwables.propagate(e);
    } finally {
      close(r);
      close(statement);
      close(conn);
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

  public void insert(String table, Iterable<Row> rows) {
    if (Iterables.isEmpty(rows)) {
      return;
    }

    Connection conn = getConnection();
    PreparedStatement statement = null;
    ResultSet generatedKeys = null;

    try {
      String s = Iterables.getFirst(rows, null).getInsertStatement(schema, table);
      statement = conn.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);
      for (Row row : rows) {
        int c = 1;
        for (Object o : row.map.values()) {
          statement.setObject(c++, convert(o));
        }
        statement.addBatch();
      }
      statement.executeBatch();
      generatedKeys = statement.getGeneratedKeys();

      Iterator<Row> iter = rows.iterator();
      while (generatedKeys.next()) {
        long id = generatedKeys.getLong(1);
        iter.next().with("id", id);
      }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      close(generatedKeys);
      close(statement);
      close(conn);
    }

  }

  public int update(String query, Object... args) {
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
      throw Throwables.propagate(e);
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
      throw Throwables.propagate(e);
    } finally {
      close(statement);
      close(conn);
    }
  }

  public int getCount(String countQuery, Object... args) {
    Row row = selectSingleRow(countQuery, args);
    return row.getLong("count(*)").intValue();
  }

  public Set<String> getSchemas() {
    Set<String> ret = Sets.newHashSet();
    Connection c = getConnection();
    try {
      ResultSet rs = c.getMetaData().getCatalogs();

      while (rs.next()) {
        ret.add(rs.getString("TABLE_CAT").toLowerCase());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      close(c);
    }
    return ret;
  }

  public boolean hasTable(String table) {
    return getTables().contains(table.toLowerCase());
  }

  public Set<String> getTables() {
    Set<String> ret = Sets.newHashSet();
    Connection c = getConnection();
    try {
      ResultSet rs = c.getMetaData().getTables(schema, null, "%", null);

      while (rs.next()) {
        ret.add(rs.getString(3).toLowerCase());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      close(c);
    }
    return ret;
  }

  public void addTable(Table table) {
    checkNotNull(table);

    if (getTables().contains(table.name)) {
      throw new IllegalArgumentException("Table already exists: " + table.name);
    }

    execute(table.toSQL(schema));
    for (List<String> index : table.indices) {
      String indexName = Joiner.on("_").join(index);
      index = map(index, s -> '`' + s + '`');
      execute("ALTER TABLE `" + table.name + "` ADD INDEX `" + indexName + "` (" + Joiner.on(",").join(index) + ")");
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
    execute("DELETE FROM " + schema + "." + table);
  }

  public void deleteTable(String table) {
    execute("DROP TABLE `" + schema + "`.`" + table + "`");
  }

  public void deleteColumn(String table, String column) {
    execute("ALTER TABLE `" + table + "` DROP COLUMN `" + column + "`");
  }

  public void execute(String statement) {
    Connection c = getConnection();
    try {
      Statement s = c.createStatement();
      s.executeUpdate(statement);
      s.close();
    } catch (Exception e) {
      System.err.println("Problem executing statement:\n " + statement);
      throw Throwables.propagate(e);
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
      throw Throwables.propagate(e);
    }
  }

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
      return Date.from((Instant) o);
    } else if (o.getClass().isEnum()) {
      Enum<?> e = (Enum<?>) o;
      return e.name();
    } else if (o instanceof Json) {
      return o.toString();
    }
    return o;
  }

}
