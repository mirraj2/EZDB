package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static ox.util.Functions.map;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import ox.Json;
import ox.Money;
import ox.Reflection;

public class Table {

  /**
   * 191 is the maximum number of utf8mb4 (4 byte) characters which is still under the max key length of 767 bytes see
   * the 2nd answer here:
   * https://stackoverflow.com/questions/1814532/1071-specified-key-was-too-long-max-key-length-is-767-bytes
   */
  public static final int MAX_STRING_SIZE = 191;

  public final String name;

  private final Map<String, String> columns = Maps.newLinkedHashMap();
  private final List<Integer> primaryIndices = Lists.newArrayList();
  private final Set<String> autoConvertColumns = Sets.newHashSet();
  final List<Index> indices = Lists.newArrayList();

  private String lastColumnAdded = "";

  public Table(String name) {
    this.name = name;
  }

  public Table column(String name, Class<?> type) {
    return column(name, getType(type));
  }

  public Table column(String name, String type) {
    columns.put(name, type.toUpperCase());
    lastColumnAdded = name;
    return this;
  }

  public Table varchar(String name, int n) {
    return column(name, "VARCHAR(" + n + ")");
  }

  /**
   * Adds an 'id' column that auto increments.
   */
  public Table idColumn() {
    primaryIndices.add(columns.size());
    column("id", "INTEGER UNSIGNED NOT NULL AUTO_INCREMENT");
    return this;
  }

  public Table primary(String name, Class<?> type) {
    return primary(name, getType(type));
  }

  public Table primary(String name, String type) {
    primaryIndices.add(columns.size());
    column(name, type);
    return this;
  }

  public Table index() {
    return index(lastColumnAdded);
  }

  public Table index(String... columns) {
    indices.add(new Index(ImmutableList.copyOf(columns), false));
    return this;
  }

  /**
   * @param autoConvertStrings Automatically converts empty strings to 'null' when storing into the database and
   *                           converts 'null' into the empty string when reading from the database. Useful for String
   *                           columns which have a unique index.
   */
  public Table uniqueIndex(boolean autoConvertStrings) {
    checkState(!lastColumnAdded.isEmpty());
    autoConvertColumns.add(lastColumnAdded);
    uniqueIndex(lastColumnAdded);
    return this;
  }

  public Table uniqueIndex() {
    return uniqueIndex(lastColumnAdded);
  }

  public Table uniqueIndex(String... columns) {
    indices.add(new Index(ImmutableList.copyOf(columns), true));
    return this;
  }

  public Collection<Index> getIndices() {
    return indices;
  }

  static String getType(Class<?> type) {
    if (type == UUID.class) {
      return "CHAR(36)";
    } else if (type == Integer.class) {
      return "INT";
    } else if (type == Double.class) {
      return "DOUBLE";
    } else if (type == Long.class || type == Instant.class || type == Money.class) {
      return "BIGINT";
    } else if (type == Boolean.class) {
      return "TINYINT(1)";
    } else if (type == String.class) {
      return "VARCHAR(" + MAX_STRING_SIZE + ")";
    } else if (type == LocalDateTime.class) {
      return "VARCHAR(63)";
    } else if (type == LocalDate.class) {
      return "DATE";
    } else if (type == LocalTime.class) {
      return "CHAR(5)";
    }
    throw new RuntimeException("Unsupported type: " + type);
  }

  public String toSQL(String schema) {
    if (columns.isEmpty()) {
      throw new RuntimeException("You must have at least one column!");
    }

    String s = "CREATE TABLE `" + schema + "`.`" + name + "`(\n";
    for (Entry<String, String> e : columns.entrySet()) {
      s += "`" + e.getKey() + "`" + " " + e.getValue() + ",\n";
    }
    if (!primaryIndices.isEmpty()) {
      s += "PRIMARY KEY (";
      for (Integer i : primaryIndices) {
        String primary = Iterables.get(columns.keySet(), i);
        s += "`" + primary + "`,";
      }
      s = s.substring(0, s.length() - 1);
      s += "),\n";
    }
    s = s.substring(0, s.length() - 2);
    s += ")\n";
    s += "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_bin;";

    return s;
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  public boolean hasColumn(String column) {
    return columns.containsKey(column);
  }

  public Row toRow(Object o) {
    checkNotNull(o);

    Row row = new Row(columns.size());

    for (String column : columns.keySet()) {
      Object value = Reflection.get(o, column);
      if (value instanceof Json) {
        value = value.toString();
      } else if (value instanceof Iterable) {
        value = Json.array((Iterable<?>) value);
      }
      if (autoConvertColumns.contains(column)) {
        if ("".equals(value)) {
          value = null;
        }
      }
      row.with(column, value);
    }

    return row;
  }

  public List<Row> toRows(Collection<?> list) {
    return map(list, this::toRow);
  }

  public <T> List<T> fromRows(Collection<Row> rows, Class<T> c) {
    return map(rows, row -> fromRow(row, c));
  }

  public <T> T fromRow(Row row, Class<T> c) {
    if (row == null) {
      return null;
    }

    T ret = Reflection.newInstance(c);
    row.map.forEach((k, v) -> {
      if (v == null && autoConvertColumns.contains(k)) {
        v = "";
      }
      Reflection.set(ret, k, v);
    });
    return ret;
  }

  @Override
  public String toString() {
    return name;
  }

  public static class Index {
    public final List<String> columns;
    public final boolean unique;

    public Index(List<String> columns, boolean unique) {
      this.columns = columns;
      this.unique = unique;
    }
  }

}
