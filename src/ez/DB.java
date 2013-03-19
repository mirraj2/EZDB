package ez;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.jolbox.bonecp.*;

public class DB {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(DB.class);

  private final BoneCP pool;

  private String schema = null;

  public DB(String ip, String user, String pass) {
    try {
      BoneCPConfig config = new BoneCPConfig();
      config.setJdbcUrl("jdbc:mysql://" + ip + ":3306/");
      config.setUsername(user);
      config.setPassword(pass);
      config.setMaxConnectionsPerPartition(20);
      pool = new BoneCP(config);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public DB usingSchema(String schema) {
    if (StringUtils.isBlank(schema)) schema = null;

    if (!getSchemas().contains(schema.toLowerCase())) {
      execute("Create Database " + schema);
    }

    this.schema = schema;

    return this;
  }

  public List<Row> select(String query) {
    Connection conn = getConnection();
    Statement statement = null;
    try {
      statement = conn.createStatement();
      ResultSet r = statement.executeQuery(query);
      List<Row> ret = Lists.newArrayList();
      ResultSetMetaData meta = r.getMetaData();
      while (r.next()) {
        Row row = new Row();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          row.with(meta.getColumnName(i), r.getObject(i));
        }
        ret.add(row);
      }
      return ret;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (statement != null) {
        close(statement);
      }
      close(conn);
    }
  }

  public void insert(String table, Iterable<Row> rows) {
    for (Row row : rows) {
      insert(table, row);
    }
  }

  public void insert(String table, Row row) {
    Connection conn = getConnection();
    PreparedStatement statement = null;
    try {
      statement = conn.prepareStatement(row.getInsertStatement(schema, table));
      int c = 1;
      for (Object o : row.map.values()) {
        statement.setObject(c++, convert(o));
      }
      statement.executeUpdate();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (statement != null) {
        close(statement);
      }
      close(conn);
    }
  }

  public void update(String table, Row row) {
    Connection conn = getConnection();
    PreparedStatement statement = null;
    String query = "";
    try {
      query = row.getUpdateStatement(schema, table);
      statement = conn.prepareStatement(query);
      int c = 1;
      for (Entry<String, Object> e : row.map.entrySet()) {
        if (e.getKey().equals("id")) continue;
        statement.setObject(c++, convert(e.getValue()));
      }
      statement.setObject(c++, convert(row.map.get("id")));
      statement.executeUpdate();
    } catch (Exception e) {
      logger.error("query: " + query);
      throw Throwables.propagate(e);
    } finally {
      if (statement != null) {
        close(statement);
      }
      close(conn);
    }
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
    if (getTables().contains(table.name)) {
      throw new IllegalArgumentException("Table already exists: " + table.name);
    }

    execute(table.toSQL(schema));
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
    execute("DROP TABLE " + schema + "." + table);
  }

  public void execute(String statement) {
    Connection c = getConnection();
    try {
      Statement s = c.createStatement();
      s.executeUpdate(statement);
      s.close();
    } catch (Exception e) {
      logger.error("Problem executing statement:\n " + statement);
      throw Throwables.propagate(e);
    } finally {
      close(c);
    }
  }

  private void close(Connection c) {
    try {
      c.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void close(Statement statement) {
    try {
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Connection getConnection() {
    try {
      return pool.getConnection();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Object convert(Object o) {
    if (o instanceof UUID) {
      return o.toString();
    }
    return o;
  }

}
