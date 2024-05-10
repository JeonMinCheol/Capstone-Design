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

package org.apache.calcite.adapter.couchdbOriginLink;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CouchTable extends AbstractTable implements ScannableTable {
  private final String dbName;
  private final CouchSchema schema;
  private final JSONParser jsonParser;
  private JSONArray docs;

  public CouchTable(CouchSchema schema, JSONParser jsonParser, String dbName) {
    this.schema = schema;
    this.jsonParser = jsonParser;
    this.dbName = dbName;
  }

  static Table create(CouchSchema schema, String tableName) {
    return new CouchTable(schema, new JSONParser(), tableName);
  }

  @Override
  public String toString() {
    return "CouchTable {" + dbName + '}';
  }

  // table의 attribute 설정
  // MAP이라는 attirbute 하나만 생성해서 때려박음
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType = typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));

    return typeFactory.builder().add(dbName, mapType).build();
  }

  // table(document)내의 row를 가져옴
  private Enumerable<Object[]> find() {
    String tableUri = schema.getDbClient().getDBUri().toString() + "/_find";
    String query = "{ \"selector\" : {} }";

    List<Object> values = new ArrayList<>();

    try {
      // document 불러오는 코드
      HttpPost req = new HttpPost(tableUri);
      HttpEntity body = new StringEntity(query);
      req.setEntity(body);
      req.addHeader("Content-Type", "application/json");

      HttpEntity res = schema.getDbClient().executeRequest(req).getEntity();
      String finds = EntityUtils.toString(res, "UTF-8");

      docs = (JSONArray) ((JSONObject) jsonParser.parse(finds)).get("docs");
      return new AbstractEnumerable<Object[]>() {
        @Override
        public Enumerator<Object[]> enumerator() {
          return new CouchEnumerator(docs);
        }
      };
    } catch (IOException | ParseException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return find();
  }


}
