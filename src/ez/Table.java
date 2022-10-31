package ez;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static ox.util.Functions.map;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
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

import ez.helper.ForeignKeyBuilder;
import ez.helper.ForeignKeyConstraint;
import ez.misc.DatabaseType;

import ox.Json;
import ox.Money;
import ox.Percent;
import ox.Reflection;
import ox.x.XList;

public class Table {

  /**
   * 191 is the maximum number of utf8mb4 (4 byte) characters which is still under the max key length of 767 bytes see
   * the 2nd answer here:
   * https://stackoverflow.com/questions/1814532/1071-specified-key-was-too-long-max-key-length-is-767-bytes
   */
  public static final int MAX_STRING_SIZE = 191;

  public static final String CASE_INSENSITIVE_COLLATION = "utf8mb4_0900_ai_ci";

  private static final Map<Class<?>, String> columnTypesMap = Maps.newConcurrentMap();
  private static final Map<Class<?>, String> postgresTypesMap = Maps.newConcurrentMap();

  public final String name;

  private final Map<String, String> columns = Maps.newLinkedHashMap();
  private final Map<String, Class<?>> columnClasses = Maps.newLinkedHashMap();

  private final List<Integer> primaryIndices = Lists.newArrayList();
  private final Set<String> autoConvertColumns = Sets.newHashSet();
  final List<Index> indices = Lists.newArrayList();

  private final List<ForeignKeyConstraint> foreignKeyConstraints = Lists.newArrayList();

  protected String lastColumnAdded = "";

  public boolean caseSensitive = true;

  private DatabaseType databaseType = DatabaseType.MYSQL;

  public Table(String name) {
    this(name, DatabaseType.MYSQL);
  }

  public Table(String name, DatabaseType databaseType) {
    this.name = name;
    this.databaseType = databaseType;
  }

  public DatabaseType getDatabaseType() {
    return databaseType;
  }

  public Table databaseType(DatabaseType databaseType) {
    this.databaseType = databaseType;
    return this;
  }

  public Table postgres() {
    this.databaseType = DatabaseType.POSTGRES;
    return this;
  }

  public Table column(String name, Class<?> type) {
    columnClasses.put(name, type);
    return column(name, getType(databaseType, type));
  }

  public Table column(String name, String type) {
    checkState(!columns.containsKey(name), "Already added column with name: " + name);
    columns.put(name, type);
    lastColumnAdded = name;
    return this;
  }

  public Table varchar(String name, int n) {
    return column(name, "VARCHAR(" + n + ")");
  }

  public Table linkColumn(String name, Class<?> type, String foreignTable, String foreignColumnName,
      String foreignKeyName) {
    columnClasses.put(name, type);
    return linkColumn(name, getType(type), foreignTable, foreignColumnName, foreignKeyName);
  }

  public Table linkColumn(String name, String type, String foreignTable, String foreignColumnName,
      String foreignKeyName) {
    foreignKeyConstraints
        .add(ForeignKeyBuilder.create(this.name, name, foreignTable, foreignColumnName, foreignKeyName));
    return column(name, type);
  }

  public Table linkColumn(String name, Class<?> type, String foreignTable, String foreignColumnName) {
    columnClasses.put(name, type);
    return linkColumn(name, getType(type), foreignTable, foreignColumnName);
  }

  public Table linkColumn(String name, String type, String foreignTable, String foreignColumnName) {
    foreignKeyConstraints.add(ForeignKeyBuilder.create(this.name, name, foreignTable, foreignColumnName));
    return column(name, type);
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
    columnClasses.put(name, type);
    return primary(name, getType(databaseType, type));
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

  public Table uniqueIndex(boolean autoConvertStrings) {
    checkState(!lastColumnAdded.isEmpty());
    autoConvertStrings();
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

  /**
   * Automatically converts empty strings to 'null' when storing into the database and converts 'null' into the empty
   * string when reading from the database. Useful for String columns which have a unique index.
   */
  public Table autoConvertStrings() {
    checkState(!lastColumnAdded.isEmpty());
    autoConvertColumns.add(lastColumnAdded);
    return this;
  }

  public Collection<Index> getIndices() {
    return indices;
  }

  public Table caseSensitive(boolean b) {
    this.caseSensitive = b;
    return this;
  }

  public static String getType(Class<?> type) {
    return getType(DatabaseType.MYSQL, type);
  }

  public static String getType(DatabaseType databaseType, Class<?> type) {
    String ret;
    if (databaseType == DatabaseType.POSTGRES) {
      ret = postgresTypesMap.get(type);
    } else {
      ret = columnTypesMap.get(type);
    }
    if (ret == null) {
      throw new RuntimeException("Unsupported type: " + type);
    }
    return ret;
  }

  public String toSQL(String schema) {
    if (columns.isEmpty()) {
      throw new RuntimeException("You must have at least one column!");
    }

    if (databaseType == DatabaseType.MYSQL) {
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
      s += "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE ";
      if (caseSensitive) {
        s += "utf8mb4_bin";
      } else {
        s += CASE_INSENSITIVE_COLLATION;
      }
      s += ";";
      return s;
    } else {
      String s = "CREATE TABLE " + databaseType.escape(name) + " (\n";
      for (Entry<String, String> e : columns.entrySet()) {
        s += databaseType.escape(e.getKey()) + " " + e.getValue() + ",\n";
      }
      s = s.substring(0, s.length() - 2);
      s += ");\n";
      return s;
    }
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  public Map<String, Class<?>> getColumnClasses() {
    return columnClasses;
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

  public XList<Row> toRows(Iterable<?> list) {
    return map(list, this::toRow);
  }

  public <T> XList<T> fromRows(Collection<Row> rows, Class<T> c) {
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

  public Set<String> getAutoConvertColumns() {
    return autoConvertColumns;
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

    @Override
    public String toString() {
      return String.format("[columns=%s, unique=%s]", columns, unique);
    }
  }

  static {
    columnTypesMap.put(UUID.class, "CHAR(36)");
    columnTypesMap.put(Integer.class, "INT");
    columnTypesMap.put(Double.class, "DOUBLE");
    columnTypesMap.put(Long.class, "BIGINT");
    columnTypesMap.put(Instant.class, "BIGINT");
    columnTypesMap.put(Money.class, "BIGINT");
    columnTypesMap.put(Boolean.class, "BOOLEAN");
    columnTypesMap.put(String.class, "VARCHAR(" + MAX_STRING_SIZE + ")");
    columnTypesMap.put(LocalDateTime.class, "CHAR(63)");
    columnTypesMap.put(LocalDate.class, "DATE");
    columnTypesMap.put(LocalTime.class, "CHAR(5)");
    columnTypesMap.put(Percent.class, "VARCHAR(20)");
    columnTypesMap.put(ZoneId.class, "VARCHAR(64)");

    postgresTypesMap.putAll(columnTypesMap);
    postgresTypesMap.put(Double.class, "double precision");
    postgresTypesMap.put(Json.class, "jsonb");
  }

  public static void registerColumn(Class<?> columnType, String sqlType) {
    columnTypesMap.put(columnType, sqlType);
  }

}
