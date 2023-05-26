package ez;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;

import ez.misc.DatabaseType;

import ox.Json;
import ox.Money;
import ox.Percent;
import ox.Reflection;
import ox.util.Utils;
import ox.x.XOptional;

public class Row implements Iterable<String> {

  public final Map<String, Object> map;

  public Row() {
    this(16);
  }

  public Row(int expectedSize) {
    map = Maps.newLinkedHashMapWithExpectedSize(expectedSize);
  }

  public boolean hasKey(String key) {
    return map.containsKey(key);
  }

  public Row with(String key, Object value) {
    map.put(key, value);
    return this;
  }

  public Long getId() {
    Object val = map.get("id");
    return (val instanceof Long) ? (Long) val : ((Integer) val).longValue();
  }

  public String get(String key) {
    return (String) map.get(key);
  }

  public Integer getInt(String key) {
    Object ret = map.get(key);
    if (ret instanceof Integer || ret == null) {
      return (Integer) ret;
    } else {
      return ((Number) ret).intValue();
    }
  }

  public Long getLong(String key) {
    Object o = map.get(key);
    if (o instanceof Long || o == null) {
      return (Long) o;
    }
    if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    if (o instanceof Instant) {
      return ((Instant) o).toEpochMilli();
    }
    throw new RuntimeException("Unhandled type: " + o.getClass());
  }

  public Instant getTimestamp(String key) {
    return Reflection.convert(map.get(key), Instant.class);
  }

  public Double getDouble(String key) {
    Object ret = map.get(key);
    if (ret instanceof Double || ret == null) {
      return (Double) ret;
    } else {
      return ((Number) ret).doubleValue();
    }
  }

  public Boolean getBoolean(String key) {
    return (Boolean) map.get(key);
  }

  public Json getJson(String key) {
    Object o = map.get(key);
    if (o instanceof Json) {
      return (Json) o;
    } else {
      return new Json((String) map.get(key));
    }
  }

  public UUID getUUID(String key) {
    return UUID.fromString(get(key));
  }

  public <T extends Enum<T>> T getEnum(String key, Class<T> enumClass) {
    return Utils.parseEnum(get(key), enumClass);
  }

  public Money getMoney(String key) {
    Long val = getLong(key);
    return val == null ? null : Money.fromLong(val);
  }

  public Percent getPercent(String key) {
    return Percent.parse(get(key));
  }

  public LocalDate getDate(String key) {
    Date date = (Date) map.get(key);
    return date == null ? null : date.toLocalDate();
  }

  public LocalDateTime getDateTime(String key) {
    String s = (String) map.get(key);
    return LocalDateTime.parse(s);
  }

  public byte[] getBlob(String key) {
    return (byte[]) map.get(key);
  }

  @SuppressWarnings("unchecked")
  public <T> T getObject(String key) {
    return (T) map.get(key);
  }

  // String getInsertStatement(DatabaseType databaseType, String schema, String table) {
  // String s = getInsertStatementFirstPart(databaseType, schema, table, false);
  // s += " VALUES (";
  // for (int i = 0; i < map.size(); i++) {
  // s += "?,";
  // }
  // s = s.substring(0, s.length() - 1);
  // s += ")";
  // return s;
  // }

  String getInsertStatementFirstPart(DatabaseType databaseType, String schema, Table table,
      XOptional<String> uniqueIndexForReplace) {
    String action = databaseType == DatabaseType.MYSQL && uniqueIndexForReplace.isPresent() ? "REPLACE" : "INSERT";
    String s = action + " INTO " + databaseType.escape(schema) + "." + databaseType.escape(table.name) + " (";
    for (String k : map.keySet()) {
      s += databaseType.escape(k) + ", ";
    }
    s = s.substring(0, s.length() - 2);
    s += ")";
    return s;
  }

  String getUpdateStatement(DatabaseType databaseType, String schema, String table) {
    String s = "UPDATE " + databaseType.escape(schema) + "." + databaseType.escape(table) + " SET ";
    for (String k : map.keySet()) {
      if (!k.equals("id")) {
        s += databaseType.escape(k) + " = ?, ";
      }
    }
    s = s.substring(0, s.length() - 2);
    s += " WHERE id = ?";
    return s;
  }

  @Override
  public String toString() {
    return map.toString();
  }

  @Override
  public Iterator<String> iterator() {
    return map.keySet().iterator();
  }

  public Row remove(String key) {
    map.remove(key);
    return this;
  }

  public Json toJson() {
    Json ret = Json.object();
    map.forEach((key, value) -> {
      ret.with(key, value);
    });
    return ret;
  }

  public Collection<Object> values() {
    return map.values();
  }

}
