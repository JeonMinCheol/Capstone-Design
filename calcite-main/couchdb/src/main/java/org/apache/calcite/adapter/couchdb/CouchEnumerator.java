package org.apache.calcite.adapter.couchdb;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;

import org.apache.calcite.linq4j.function.Function1;

import org.apache.calcite.linq4j.tree.Primitive;

import org.apache.calcite.plan.Convention;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CouchEnumerator implements Enumerator<Object> {
  private final Iterator<JSONObject> cursor;
  private final Function1<JSONObject, Object> getter;
  private final JSONArray json;
  private Object current;
  private final boolean selectAll;
  private int index = 0;

  public CouchEnumerator(JSONArray json, Function1<JSONObject, Object> getter, boolean selectAll) {
    this.getter = getter;
    this.json = json;
    this.selectAll = selectAll;
    this.cursor = json.iterator(); // JSONObject 반환
  }

  // TODO : Error: java.lang.ClassCastException 해결하기
  @Override
  public Object current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    try {
      if(selectAll && index < json.size()){
        current = json.get(index++);
        return true;
      }
      else if (cursor.hasNext()) {
        JSONObject map = cursor.next();
        current = getter.apply(map);
      } else {
        current = null;
        return false;
      }
      return true;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
  }

  @Override
  public void close() {
  }

  final boolean isSelectAll(String name) {
    return "_MAP".equals(name);
  }


  /** Returns a function that projects a single field. */
  static Function1<JSONObject, Object> singletonGetter(final String fieldName,
      final Class fieldClass) {

    return a0 -> convert(a0.get(
        CouchRel.Implementor.getExpressionItemMapping().getOrDefault(fieldName, fieldName)),
        fieldClass);
  }

  /** Returns a function that projects fields.
   *
   * @param fields List of fields to project; or null to return map
   */
  static Function1<JSONObject, Object[]> listGetter(
      final List<Map.Entry<String, Class>> fields) {
    return a0 -> {
      Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        final Map.Entry<String, Class> field = fields.get(i);
        final String name =  field.getKey();
        objects[i] = convert(a0.get(CouchRel.Implementor.getExpressionItemMapping().getOrDefault(name, name)), field.getValue());
      }
      return objects;
    };
  }

  static Function1<JSONObject, Object> getter(
      List<Map.Entry<String, Class>> fields) {
    //noinspection unchecked
    return fields.size() == 1
            ? singletonGetter(CouchRel.Implementor.getExpressionItemMapping().get(fields.get(0).getKey()) , fields.get(0).getValue())
            : (Function1) listGetter(fields);
  }

  @SuppressWarnings("JavaUtilDate")
  private static Object convert(Object o, Class clazz) {
    Primitive primitive = Primitive.of(clazz);
    if (primitive != null) {
      clazz = primitive.boxClass;
    } else {
      primitive = Primitive.ofBox(clazz);
    }
    if (clazz.isInstance(o)) {
      return o;
    }
    if (o instanceof Date && clazz == Long.class) {
      o = ((Date) o).getTime();
    } else if (o instanceof Date && primitive != null) {
      o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
    }
    if (o instanceof Number && primitive != null) {
      return primitive.number((Number) o);
    }
    return o;
  }
}
