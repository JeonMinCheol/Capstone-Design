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

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.linq4j.tree.Types;

import java.lang.reflect.Method;
import java.util.List;

public enum CouchMethod {
  // CouchTable.find method

  // TODO : 만들고 변경
  COUCH_QUERYABLE_FIND(CouchTable.CouchQueryable.class,
      "find",
      List.class,
      String.class,
      String.class,
      String.class
//      Long.class
  );

  public final Method method;
  public static final ImmutableMap<Method, CouchMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, CouchMethod> builder = ImmutableMap.builder();
    for (CouchMethod value : CouchMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }
  CouchMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz,methodName,argumentTypes);
  }
}
