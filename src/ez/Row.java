package ez;

import java.util.Map;

import com.google.common.collect.Maps;

public class Row {

  final Map<String, Object> map = Maps.newLinkedHashMap();

  public Row with(String key, Object value) {
    map.put(key, value);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key) {
    return (T) map.get(key);
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

}
