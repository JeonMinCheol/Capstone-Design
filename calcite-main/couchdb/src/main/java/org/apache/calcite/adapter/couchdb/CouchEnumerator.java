package org.apache.calcite.adapter.couchdb;

import org.apache.calcite.linq4j.Enumerator;

import org.json.simple.JSONArray;

public class CouchEnumerator implements Enumerator {
  private final JSONArray rows;
  private Object current;

  private int index = 0;
  public CouchEnumerator(JSONArray rows) {
    this.rows = rows;
  }

  @Override
  public Object current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    current = null;

    if(index < rows.size()) {
      current = rows.get(index);
      index++;
    }
    else
      return false;

    return true;
  }

  @Override
  public void reset() {
  }

  @Override
  public void close() {
  }

}
