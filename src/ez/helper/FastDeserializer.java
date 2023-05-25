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
      FieldDeserializer fieldDeserializer = fieldDeserializers.computeIfAbsent(k, FieldDeserializer::new);
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

    public FieldDeserializer(String fieldName) {
      this.fieldName = fieldName;
      if (table.autoConvertColumns.contains(fieldName)) {
        applier = this::applyWithAutoConvert;
      } else {
        applier = this::apply;
      }
      field = Reflection.getField(c, fieldName);
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

  }

}
