package ez;

import jasonlib.Json;
import jasonlib.util.Utils;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class Row implements Iterable<String> {

  final Map<String, Object> map = Maps.newLinkedHashMap();

  public Row with(String key, Object value) {
    map.put(key, value);
    return this;
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

  public <T extends Enum<T>> T getEnum(String key, Class<T> enumClass) {
    return Utils.parseEnum(get(key), enumClass);
  }

  public LocalDate getDate(String key) {
    Date date = (Date) map.get(key);
    return date == null ? null : date.toLocalDate();
  }

  public LocalDateTime getDateTime(String key) {
    String s = (String) map.get(key);
    return LocalDateTime.parse(s);
  }

  public Object getObject(String key) {
    return map.get(key);
  }

  String getInsertStatement(String schema, String table) {
    String s = "INSERT INTO `" + schema + "`.`" + table + "` (";
    for (String k : map.keySet()) {
      s += "`" + k + "`, ";
    }
    s = s.substring(0, s.length() - 2);
    s += ") VALUES (";
    for (int i = 0; i < map.size(); i++) {
      s += "?,";
    }
    s = s.substring(0, s.length() - 1);
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

  public Map<String, Object> getMap() {
    return map;
  }

  public Json toJson() {
    return toJson(ImmutableMap.of());
  }

  public Json toJson(Map<String, String> keyTransform) {
    Json ret = Json.object();
    getMap().forEach((key, value) -> {
      key = keyTransform.getOrDefault(key, key);
      add(ret, key, value);
    });
    return ret;
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
      json.with(key, (Boolean) value);
    } else if (value instanceof Date) {
      json.with(key, value.toString());
    } else if (value instanceof Timestamp) {
      json.with(key, value.toString());
    } else {
      throw new RuntimeException("Unhandled type: " + value.getClass());
    }
  }

}
