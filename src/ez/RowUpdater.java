package ez;

import static com.google.common.collect.Iterables.getFirst;
import static ox.util.Utils.abbreviate;
import static ox.util.Utils.propagate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Map.Entry;

import ox.Log;
import ox.x.XList;

public class RowUpdater {

  public int update(DB db, String query, Object... args) {
    query = db.appendTraceId(query);
    db.log(query, args);

    Connection conn = db.getConnection(true);
    PreparedStatement statement = null;
    try {
      statement = conn.prepareStatement(query);
      int c = 1;
      for (Object arg : args) {
        statement.setObject(c++, DB.convert(arg));
      }
      return statement.executeUpdate();
    } catch (Exception e) {
      Log.error("query: " + abbreviate(query, 1024));
      throw propagate(e);
    } finally {
      db.close(statement);
      db.close(conn);
    }
  }

  public void update(DB db, String table, Collection<Row> rows) {
    if (rows.isEmpty()) {
      return;
    }

    Connection conn = db.getConnection(true);
    PreparedStatement statement = null;
    String query = "";
    try {
      query = getFirst(rows, null).getUpdateStatement(db.databaseType, db.getSchema(), table);
      query = db.appendTraceId(query);
      // db.log(query);

      statement = conn.prepareStatement(query);
      XList<Object> args = XList.create();
      for (Row row : rows) {
        int c = 1;
        for (Entry<String, Object> e : row.map.entrySet()) {
          if (e.getKey().equals("id")) {
            continue;
          }
          Object o = DB.convert(e.getValue());
          args.add(o);
          statement.setObject(c++, o);
        }
        Object o = DB.convert(row.map.get("id"));
        args.add(o);
        statement.setObject(c++, o);
        if (DB.debug) {
          db.log(query, args.toArray());
        }
        args.clear();
        statement.addBatch();
      }
      statement.executeBatch();
    } catch (Exception e) {
      System.err.println("query: " + query);
      throw propagate(e);
    } finally {
      db.close(statement);
      db.close(conn);
    }
  }

}
