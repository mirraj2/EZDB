package ez.helper;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import ez.Row;
import ez.Table;

import ox.Log;
import ox.Reflection;

public class FastDeserializer<T> {

  private final Table table;
  private final Class<T> c;
  private final Map<String, FieldDeserializer> fieldDeserializers = Maps.newConcurrentMap();

  public FastDeserializer(Table table, Class<T> c) {
    this.table = table;
    this.c = c;
  }

  public T deserialize(Row row) {
    if (row == null) {
      return null;
    }

    T ret = Reflection.newInstance(c);

    row.map.forEach((k, v) -> {
      FieldDeserializer fieldDeserializer = fieldDeserializers.computeIfAbsent(k.toLowerCase(), FieldDeserializer::new);
      fieldDeserializer.applier.accept(ret, v);
    });

    return ret;
  }

  private class FieldDeserializer {

    private final String fieldName;
    private BiConsumer<T, Object> applier;
    private final Field field;
    private final Type targetType;
    private final Class<?> wrappedClass;
    private Object sentinalValue;

    public FieldDeserializer(String fieldNameArg) {
      String classFieldName = findClassFieldName(fieldNameArg, c); // looking for corresponding field in class

      if (classFieldName != null) {
        field = Reflection.getField(c, classFieldName);
        this.fieldName = classFieldName;
      } else {
        this.fieldName = fieldNameArg;
        field = null;
      }
      sentinalValue = table.columnSentinalValues.get(this.fieldName.toLowerCase()); // Note the change here

      if (sentinalValue != null) {
        applier = this::applyWithSentinalValue;
      } else {
        if (table.autoConvertColumns.contains(this.fieldName)) {
          applier = this::applyWithAutoConvert;
        } else {
          applier = this::apply;
        }
      }

      if (field == null) {
        applier = (object, value) -> {
          // no-op
        };
        targetType = wrappedClass = null;
      } else {
        targetType = field.getGenericType();
        wrappedClass = TypeToken.of(targetType).getRawType();
      }
    }

    private String findClassFieldName(String tableFieldName, Class<?> startClass) {
      Class<?> currentClass = startClass;
      while (currentClass != null) {
        for (Field field : currentClass.getDeclaredFields()) {
          if (field.getName().equalsIgnoreCase(tableFieldName)) {
            return field.getName();
          }
        }
        currentClass = currentClass.getSuperclass();
      }
      return null;
    }

    private void apply(T ret, Object value) {
      try {
        value = Reflection.convert(value, targetType, wrappedClass);
        field.set(ret, value);
      } catch (Exception e) {
        Log.error("Problem setting field: " + fieldName);
        throw new RuntimeException(e);
      }
    }

    private void applyWithAutoConvert(T ret, Object value) {
      if (value == null) {
        apply(ret, "");
      } else {
        apply(ret, value);
      }
    }

    private void applyWithSentinalValue(T ret, Object value) {
      if (sentinalValue.equals(value)) {
        apply(ret, null);
      } else {
        apply(ret, value);
      }
    }

  }

}
