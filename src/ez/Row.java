package ez;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
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

  public LocalDate getDate(String key) {
    Date date = (Date) map.get(key);
    return date == null ? null : date.toLocalDate();
  }

  public LocalDateTime getDateTime(String key) {
    Timestamp date = (Timestamp) map.get(key);
    return date == null ? null : date.toLocalDateTime();
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

}
