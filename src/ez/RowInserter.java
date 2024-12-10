package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static ox.util.Utils.first;
import static ox.util.Utils.normalize;
import static ox.util.Utils.propagate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

import ez.impl.PostgresDB;

import ox.Log;
import ox.x.XOptional;
import ox.x.XSet;

public class RowInserter {

  public void insert(DB db, Table table, List<Row> rows, int chunkSize, XOptional<ReplaceOptions> replaceOptions) {
    if (Iterables.isEmpty(rows)) {
      return;
    }

    // break the inserts into chunks
    if (rows.size() > chunkSize) {
      for (int i = 0; i < rows.size(); i += chunkSize) {
        List<Row> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
        Stopwatch watch = Stopwatch.createStarted();
        insert(db, table, chunk, chunkSize, replaceOptions);
        Log.info("Inserted " + chunk.size() + " rows into " + table + " (" + watch + ")");
      }
      return;
    }

    Connection conn = db.getConnection(true);
    PreparedStatement statement = null;
    ResultSet generatedKeys = null;

    try {
      Row firstRow = first(rows);
      StringBuilder sb = new StringBuilder(
          firstRow.getInsertStatementFirstPart(db.databaseType, db.getSchema(), table,
              replaceOptions.map(o -> o.uniqueIndex), replaceOptions.map(o -> o.ignoreDuplicates)));
      sb.append(" VALUES ");

      final String placeholders = getInsertPlaceholders(table, firstRow);
      for (int i = 0; i < rows.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(placeholders);
      }

      replaceOptions.ifPresent(o -> {
        if (db instanceof PostgresDB) {
          checkState(!o.uniqueIndex.isEmpty(), "Postgres requires specifying the uniqueIndex for a replace.");
          sb.append(" ON CONFLICT (" + db.escape(o.uniqueIndex) + ") DO UPDATE SET ");
          for (String col : firstRow) {
            if (!col.equalsIgnoreCase(o.uniqueIndex) && !o.columnsToIgnore.contains(col)) {
              String escaped = db.escape(col);
              sb.append(escaped).append("= excluded.").append(escaped).append(", ");
            }
          }
          sb.setLength(sb.length() - 2);
        }
      });

      String s = sb.toString();
      s = db.appendTraceId(s);
      db.log(s);

      statement = conn.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);

      int c = 1;
      for (Row row : rows) {
        for (Object o : row.map.values()) {
          Object converted = DB.convert(o);
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
      db.close(generatedKeys);
      db.close(statement);
      db.close(conn);
    }
  }

  private String getInsertPlaceholders(Table table, Row row) {
    final StringBuilder sb = new StringBuilder("(");
    for (String key : row) {
      String columnType = table.getColumns().get(key);
      if ("jsonb".equals(columnType)) {
        sb.append("?::JSON");
      } else {
        sb.append('?');
      }
      sb.append(',');
    }
    if (sb.length() > 1) {
      sb.setCharAt(sb.length() - 1, ')');
    } else {
      sb.append(')');
    }
    return sb.toString();
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

  public void insertRawRows(DB db, String table, List<? extends Iterable<?>> rows, boolean replace) {
    if (rows.isEmpty()) {
      return;
    }

    Connection conn = db.getConnection(true);
    PreparedStatement statement = null;

    try {
      StringBuilder sb = new StringBuilder((replace ? "REPLACE" : "INSERT") +
          " INTO `" + db.getSchema() + "`.`" + table + "` VALUES ");

      final String placeholders = getInsertPlaceholders(Iterables.size(rows.get(0)));
      for (int i = 0; i < rows.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(placeholders);
      }

      String s = sb.toString();
      s = db.appendTraceId(s);
      db.log(s);

      statement = conn.prepareStatement(s, Statement.NO_GENERATED_KEYS);

      int c = 1;
      for (Iterable<? extends Object> row : rows) {
        for (Object o : row) {
          if (o == DB.NULL) {
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
      db.close(statement);
      db.close(conn);
    }
  }

  public static class ReplaceOptions {
    public final String uniqueIndex;
    public final XSet<String> columnsToIgnore;
    public final boolean ignoreDuplicates;

    public ReplaceOptions(String uniqueIndex, XSet<String> columnsToIgnore, boolean ignoreDuplicates) {
      this.uniqueIndex = normalize(uniqueIndex);
      this.columnsToIgnore = checkNotNull(columnsToIgnore);
      this.ignoreDuplicates = ignoreDuplicates;
    }
  }

}
