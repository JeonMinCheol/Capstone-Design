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
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CouchTable extends AbstractTable implements ScannableTable {
  private final List<Object> list = new ArrayList<>();
  private final CouchSchema schema;
  private final RelProtoDataType protoRowType;
  private final JSONParser jsonParser;
  private final String documentId;
  CouchEnumerator couchEnumerator;

  public CouchTable(CouchSchema schema, RelProtoDataType protoRowType, JSONParser jsonParser,
      String documentId) {
    this.schema = schema;
    this.protoRowType = protoRowType;
    this.jsonParser = jsonParser;
    this.documentId = documentId;
  }

  static Table create(CouchSchema schema, String tableName, RelProtoDataType protoRowType) {
    return new CouchTable(schema, protoRowType, new JSONParser(), tableName);
  }

  @Override
  public String toString() {
    return "CouchTable {" + documentId + '}';
  }

  // table의 attribute 설정
  // MAP이라는 attirbute 하나만 생성해서 때려박음
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType = typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));

    return typeFactory.builder().add(documentId, mapType).build();
  }

  // table(document)내의 row를 가져옴
  private void find() {
    String tableUri = schema.getDbClient().getDBUri() + "/" + documentId;

    // document 불러오는 코드
    HttpGet docs = new HttpGet(tableUri);
    HttpEntity entity = schema.getDbClient().executeRequest(docs).getEntity();
    List<Object> values = new ArrayList<>();

    try {
      String res = EntityUtils.toString(entity, "UTF-8");
      JSONObject rows = (JSONObject) jsonParser.parse(res);

      int row = 0;
      Map<String, String> map = new ObjectMapper().readValue(rows.toJSONString(), Map.class);


      try {
        values = Arrays.asList(map.entrySet().toArray());
        for (Object o : values) {
          list.add(o);
        }
        System.out.println(list);
        } catch (Exception e) {
        throw new RuntimeException();
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Enumerable<@Nullable Object[]> scan(DataContext root) {

    find();
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new CouchEnumerator(list);
      }
    };
  }


}
