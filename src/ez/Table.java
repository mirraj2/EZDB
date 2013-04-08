package ez;

import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.*;

public class Table {

  public final String name;

  private final Map<String, String> columns = Maps.newLinkedHashMap();
  private List<Integer> primaryIndices = Lists.newArrayList();

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

  public Table primary(String name, Class<?> type) {
    primaryIndices.add(columns.size());
    columns.put(name, getType(type));
    return this;
  }

  private String getType(Class<?> type) {
    if (type == UUID.class) {
      return "CHAR(36)";
    } else if (type == Integer.class) {
      return "INT";
    } else if (type == Double.class) {
      return "DOUBLE";
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
    for (Integer i : primaryIndices) {
      String primary = Iterables.get(columns.keySet(), i);
      s += "PRIMARY KEY (`" + primary + "`),\n";
    }
    s = s.substring(0, s.length() - 2);
    s += ")\n";
    s += "ENGINE = InnoDB";

    return s;
  }

}
