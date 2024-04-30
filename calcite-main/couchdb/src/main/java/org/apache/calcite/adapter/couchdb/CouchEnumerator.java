/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.couchdb;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;


import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;

import org.apache.http.util.EntityUtils;

import org.lightcouch.CouchDbClient;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class CouchEnumerator implements Enumerator {
  private final List<Object> rows;
  private Object current;

  private int index = 0;
  public CouchEnumerator(List<Object> rows) {
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
