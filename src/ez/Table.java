package ez;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static ox.util.Functions.map;
import static ox.util.Utils.normalize;

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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import ez.helper.FastDeserializer;
import ez.helper.ForeignKeyConstraint;
import ez.misc.DatabaseType;

import ox.Json;
import ox.Money;
import ox.Percent;
import ox.Reflection;
import ox.x.XList;
import ox.x.XOptional;

import io.github.sebasbaumh.postgis.PGgeometry;

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

  public static DatabaseType DEFAULT_DATABASE_TYPE = DatabaseType.MYSQL;

  public final String name;

  private final Map<String, String> columns = Maps.newLinkedHashMap();
  private final Map<String, Class<?>> columnClasses = Maps.newLinkedHashMap();
  private final Map<String, String> columnComments = Maps.newLinkedHashMap();

  private final List<Integer> primaryIndices = Lists.newArrayList();
  public final Set<String> autoConvertColumns = Sets.newHashSet();
  public final Map<String, Object> columnSentinalValues = Maps.newHashMap();
  final List<Index> indices = Lists.newArrayList();

  private final Set<ForeignKeyConstraint> foreignKeyConstraints = Sets.newHashSet();

  protected String lastColumnAdded = "";

  public boolean caseSensitive = true;

  private DatabaseType databaseType;

  private Map<Class<?>, FastDeserializer<?>> deserializers = Maps.newConcurrentMap();

  public Table(String name) {
    this(name, DEFAULT_DATABASE_TYPE);
  }

  public Table(String name, DatabaseType databaseType) {
    this.name = name;
    this.databaseType = checkNotNull(databaseType);
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

  public Table comment(String comment) {
    return comment(lastColumnAdded, comment);
  }

  public Table comment(String columnName, String comment) {
    columnComments.put(columnName, normalize(comment).replace("'", "\\'"));
    return this;
  }

  public Table varchar(String name, int n) {
    return column(name, "VARCHAR(" + n + ")");
  }

  public Table linkColumn(String name, Class<?> type, String foreignTable, String foreignColumnName) {
    columnClasses.put(name, type);
    return linkColumn(name, getType(type), foreignTable, foreignColumnName);
  }

  public Table linkColumn(String name, String type, String foreignTable, String foreignColumnName) {
    foreignKeyConstraints.add(new ForeignKeyConstraint(this.name, name, foreignTable, foreignColumnName));
    return column(name, type);
  }

  /**
   * Adds an 'id' column that auto increments.
   */
  public Table idColumn() {
    primaryIndices.add(columns.size());
    if (databaseType == DatabaseType.POSTGRES) {
      column("id", "SERIAL PRIMARY KEY");
    } else {
      column("id", "INTEGER UNSIGNED NOT NULL AUTO_INCREMENT");
    }
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
    indices.add(new Index(XList.of(columns), false));
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
    indices.add(new Index(XList.of(columns), true));
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

  /**
   * Stores NULL as a special sentinel value. This can be useful when performing a unique index across multiple columns.
   *
   * If you have two rows: [AValue, NULL] [AValue, NULL], MySQL would normally allow both of these rows even if there
   * was a unique index b/c NULL is treated as "unknown". By using this method, we can get MySQL to throw an exception
   * when there is duplicate data.
   */
  public Table sentinelNullValue(Object sentinelValue) {
    checkArgument(sentinelValue != null, "sentinelValue cannot be null!");
    checkState(!lastColumnAdded.isEmpty());
    columnSentinalValues.put(lastColumnAdded, sentinelValue);
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
    final Class<?> originalType = type;
    String ret = null;
    while (ret == null && type != null) {
      if (databaseType == DatabaseType.POSTGRES) {
        ret = postgresTypesMap.get(type);
      } else {
        ret = columnTypesMap.get(type);
      }
      type = type.getSuperclass();
    }
    if (ret == null) {
      throw new RuntimeException("Unsupported type: " + originalType);
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
        String comment = normalize(columnComments.get(e.getKey()));
        if (!comment.isEmpty()) {
          comment = " COMMENT '" + comment + "'";
        }
        s += "`" + e.getKey() + "`" + " " + e.getValue() + comment + ",\n";
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
      s += createForeignKeys();
      s = s.substring(0, s.length() - 2);
      s += ")\n";
      s += "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE ";
      if (caseSensitive) {
        s += "utf8mb4_bin";
      } else {
        s += CASE_INSENSITIVE_COLLATION;
      }
      return s;
    } else {
      String s = "CREATE TABLE " + databaseType.escape(schema) + "." + databaseType.escape(name) + " (\n";
      for (Entry<String, String> e : columns.entrySet()) {
        s += databaseType.escape(e.getKey()) + " " + e.getValue() + ",\n";
      }
      s += createForeignKeys();
      s = s.substring(0, s.length() - 2);
      s += ")\n";
      return s;
    }
  }

  private String createForeignKeys() {
    String s = "";
    for (ForeignKeyConstraint fk : foreignKeyConstraints) {
      s += fk.getCreationStatement(databaseType);
      s += ",\n";
    }
    return s;
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
      } else if (value instanceof XOptional && ((XOptional<?>) value).isEmpty()) {
        value = null;
      }
      if (null == value) {
        value = columnSentinalValues.get(column);
      } else if ("".equals(value)) {
        if (autoConvertColumns.contains(column)) {
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

  @SuppressWarnings("unchecked")
  public <T> XList<T> fromRows(Collection<Row> rows, Class<T> c) {
    FastDeserializer<T> deserializer = (FastDeserializer<T>) deserializers.computeIfAbsent(c,
        (Class<?> cc) -> new FastDeserializer<T>(this, (Class<T>) cc));
    return map(rows, deserializer::deserialize);
  }

  @SuppressWarnings("unchecked")
  public <T> T fromRow(Row row, Class<T> c) {
    FastDeserializer<T> deserializer = (FastDeserializer<T>) deserializers.computeIfAbsent(c,
        (Class<?> cc) -> new FastDeserializer<T>(this, (Class<T>) cc));
    return deserializer.deserialize(row);
  }

  public Set<String> getAutoConvertColumns() {
    return autoConvertColumns;
  }

  @Override
  public String toString() {
    return name;
  }

  public static class Index {
    public final XList<String> columns;
    public final boolean unique;

    public Index(XList<String> columns, boolean unique) {
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
    columnTypesMap.put(Enum.class, "VARCHAR(" + MAX_STRING_SIZE + ")");
    columnTypesMap.put(LocalDateTime.class, "CHAR(63)");
    columnTypesMap.put(LocalDate.class, "DATE");
    columnTypesMap.put(LocalTime.class, "CHAR(5)");
    columnTypesMap.put(Percent.class, "VARCHAR(20)");
    columnTypesMap.put(ZoneId.class, "VARCHAR(64)");

    postgresTypesMap.putAll(columnTypesMap);
    postgresTypesMap.put(Double.class, "double precision");
    postgresTypesMap.put(Json.class, "jsonb");
    postgresTypesMap.put(PGgeometry.class, "geometry");
    postgresTypesMap.put(String.class, "TEXT");
    postgresTypesMap.put(Enum.class, "TEXT");
  }

  public static void registerColumn(Class<?> columnType, String sqlType) {
    columnTypesMap.put(columnType, sqlType);
  }

  public static void registerPostgresColumn(Class<?> columnType, String sqlType) {
    postgresTypesMap.put(columnType, sqlType);
  }

}
