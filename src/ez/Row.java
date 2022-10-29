package ez;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import ez.DB.DatabaseType;

import ox.Json;
import ox.Money;
import ox.Percent;
import ox.util.Utils;

public class Row implements Iterable<String> {

  final Map<String, Object> map;

  public Row() {
    this(16);
  }

  public Row(int expectedSize) {
    map = Maps.newLinkedHashMapWithExpectedSize(expectedSize);
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
    return (Integer) map.get(key);
  }

  public Long getLong(String key) {
    return (Long) map.get(key);
  }

  public Double getDouble(String key) {
    return (Double) map.get(key);
  }

  public Boolean getBoolean(String key) {
    return (Boolean) map.get(key);
  }

  public Json getJson(String key) {
    return new Json((String) map.get(key));
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

  String getInsertStatementFirstPart(DatabaseType databaseType, String schema, Table table, boolean replace) {
    String action = replace ? "REPLACE" : "INSERT";
    String s = action + " INTO " + databaseType.escape(schema) + "." + databaseType.escape(table.name) + " (";
    for (String k : map.keySet()) {
      s += databaseType.escape(k) + ", ";
    }
    s = s.substring(0, s.length() - 2);
    s += ")";
    return s;
  }

  String getUpdateStatement(String schema, String table) {
    String s = "UPDATE `" + schema + "`.`" + table + "` SET ";
    for (String k : map.keySet()) {
      if (!k.equals("id")) {
        s += "`" + k + "` = ?, ";
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
    return toJson(ImmutableMap.of());
  }

  public Json toJson(Map<String, String> keyTransform) {
    Json ret = Json.object();
    map.forEach((key, value) -> {
      key = keyTransform.getOrDefault(key, key);
      add(ret, key, value);
    });
    return ret;
  }

  public Collection<Object> values() {
    return map.values();
  }

  private void add(Json json, String key, Object value) {
    if (value == null) {
      return;
    }

    if (value instanceof String) {
      json.with(key, (String) value);
    } else if (value instanceof Number) {
      json.with(key, (Number) value);
    } else if (value instanceof Boolean) {
      json.with(key, value);
    } else if (value instanceof Date) {
      json.with(key, value.toString());
    } else if (value instanceof Timestamp) {
      json.with(key, value.toString());
    } else {
      throw new RuntimeException("Unhandled type: " + value.getClass());
    }
  }

}
