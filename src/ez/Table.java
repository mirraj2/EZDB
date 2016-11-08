package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static ox.util.Functions.map;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ox.Json;
import ox.Money;
import ox.Reflection;

public class Table {

  public final String name;

  private final Map<String, String> columns = Maps.newLinkedHashMap();
  private List<Integer> primaryIndices = Lists.newArrayList();
  final List<Index> indices = Lists.newArrayList();

  public Table(String name) {
    this.name = name;
  }

  public Table column(String name, Class<?> type) {
    return column(name, getType(type));
  }

  public Table column(String name, String type) {
    columns.put(name, type);
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
    columns.put(name, type);
    return this;
  }

  public Table index(String... columns) {
    indices.add(new Index(ImmutableList.copyOf(columns), false));
    return this;
  }

  public Table uniqueIndex(String... columns) {
    indices.add(new Index(ImmutableList.copyOf(columns), true));
    return this;
  }

  static String getType(Class<?> type) {
    if (type == UUID.class) {
      return "CHAR(36)";
    } else if (type == Integer.class || type == Money.class) {
      return "INT";
    } else if (type == Double.class) {
      return "DOUBLE";
    } else if (type == Long.class) {
      return "BIGINT";
    } else if (type == Boolean.class) {
      return "TINYINT(1)";
    } else if (type == String.class) {
      return "VARCHAR(255)";
    } else if (type == LocalDateTime.class) {
      return "VARCHAR(63)";
    } else if (type == LocalDate.class) {
      return "DATE";
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
    s += "ENGINE=InnoDB DEFAULT CHARSET=utf8;";

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

    Row row = new Row();

    for (String column : columns.keySet()) {
      Object value = Reflection.get(o, column);
      if (value instanceof Json) {
        value = value.toString();
      } else if (value instanceof Iterable) {
        value = Json.array((Iterable<?>) value);
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
      Reflection.set(ret, k, v);
    });
    return ret;
  }

  @Override
  public String toString() {
    return name;
  }

  static class Index {
    public final List<String> columns;
    public final boolean unique;

    public Index(List<String> columns, boolean unique) {
      this.columns = columns;
      this.unique = unique;
    }
  }

}
