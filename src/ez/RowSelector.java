package ez;

import static ox.util.Utils.propagate;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

import org.postgresql.util.PGobject;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import ez.impl.MySQLDB;
import ez.impl.PostgresDB;

import ox.Json;
import ox.Log;
import ox.x.XOptional;

public class RowSelector {

  public void select(DB db, String query, Consumer<ResultSet> rowCallback, XOptional<Integer> fetchSize,
      Object... args) {
    Stopwatch watch = Stopwatch.createStarted();

    Connection conn = db.getConnection();
    PreparedStatement statement = null;
    ResultSet r = null;
    try {
      statement = conn.prepareStatement(query);

      if (db instanceof PostgresDB) {
        // postgres fetchsize doesn't work without this line
        if (fetchSize.isPresent()) {
          conn.setAutoCommit(false);
          statement.setFetchSize(fetchSize.get());
        }
      } else if (db instanceof MySQLDB) {
        if (fetchSize.isPresent()) {
          // mysql doesn't respect the fetchsize, but setting to MIN_VALUE will cause it to stream results
          statement.setFetchSize(Integer.MIN_VALUE);
        }
      } else {
        statement.setFetchSize(fetchSize.orElse(10_000));
      }

      for (int c = 0; c < args.length; c++) {
        statement.setObject(c + 1, DB.convert(args[c]));
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
      db.log(query, watch, args);
      db.close(r);
      db.close(statement);
      db.close(conn);
    }
  }

  public void stream(DB db, String query, XOptional<Integer> fetchSize, boolean reuseRows, Consumer<Row> callback,
      Object... args) {
    Row row = new Row();
    List<String> labels = Lists.newArrayList();
    select(db, query, r -> {
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
          Object val;
          try {
            val = r.getObject(i);
          } catch (Exception e) {
            throw new RuntimeException("Problem calling getObject on " + labels.get(i - 1), e);
          }
          if (val instanceof Clob) {
            Clob clob = (Clob) val;
            val = clob.getSubString(1, Math.toIntExact(clob.length()));
          } else if (val instanceof PGobject) {
            PGobject o = (PGobject) val;
            String type = o.getType();
            if (type.equals("jsonb")) {
              val = new Json(o.getValue());
            } else {
              throw new RuntimeException("Unhandled case: " + type);
            }
          }
          theRow.with(labels.get(i - 1), val);
        }
        callback.accept(theRow);
      } catch (SQLException e) {
        throw propagate(e);
      }
    }, fetchSize, args);
  }

  public static void main(String[] args) {
    DB db = new MySQLDB("localhost", "root", "", "ender.com");
    for (int i = 0; i < 1000; i++) {
      Stopwatch watch = Stopwatch.createStarted();
      db.select("SELECT * FROM gl_tx").forEach(row -> {
      });
      Log.debug(i + " took " + watch);
    }
  }

}
