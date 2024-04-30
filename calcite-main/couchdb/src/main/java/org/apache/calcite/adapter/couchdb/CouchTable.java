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

import com.google.gson.JsonArray;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.lightcouch.CouchDbClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class CouchTable extends AbstractQueryableTable implements TranslatableTable {
  private final String dbName;
  private final JSONParser jsonParser;
  CouchTable(String dbName, JSONParser jsonParser){
      super(Object[].class);
      this.dbName = dbName;
      this.jsonParser = jsonParser;
  }

  @Override
  public String toString() {return "CouchTable {" + dbName + '}';}

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new CouchQueryable<>(queryProvider, schema, this, tableName);
  }

  // table의 attribute 설정
  // MAP이라는 attirbute 하나만 생성
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true
            )
        );

    return typeFactory.builder().add(dbName, mapType).build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CouchTableScan(cluster, cluster.traitSet(), relOptTable, this, null);
  }

  // filter
  // field
  // table(db)내의 row(docs)를 가져와 Enumerator를 반환
  private Enumerable<Object> find(CouchDbClient dbClient, List<Map.Entry<String, Class>> fields,
      List<String> ops,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      Long skip) {
    String tableUri = dbClient.getDBUri().toString()+"/_find";

    // TODO : query로 변환하는 코드

    // document 불러오는 코드
    try{
      String query = null;

      HttpPost req = new HttpPost(tableUri);
      HttpEntity body = new StringEntity(query);
      req.setEntity(body);

      HttpEntity res = dbClient.executeRequest(req).getEntity();

      String finds  = EntityUtils.toString(res,"UTF-8");
      JsonArray docs = (JsonArray) ((JSONObject) jsonParser.parse(finds)).get("docs");

      return new AbstractEnumerable<Object>() {
        @Override
        public Enumerator<Object> enumerator() {
          return new CouchEnumerator(Collections.singletonList(docs));
        }
      };

    } catch (IOException | ParseException e) {
        throw new RuntimeException(e);
    }
  }

  public class CouchQueryable<T> extends AbstractTableQueryable<T> {
    public CouchQueryable(QueryProvider queryProvider, SchemaPlus schema, CouchTable couchTable,
        String tableName) {
      super(queryProvider, schema, couchTable, tableName);
    }

    // 현재 테이블의 enumerator를 반환
    @Override
    public Enumerator<T> enumerator() {
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().find(getClient(),null, null, null, null);

      return enumerable.enumerator();
    }

    // CouchTable을 반환
    private CouchTable getTable() {
      return (CouchTable) table;
    }

    // CouchDBClient 반환
    private CouchDbClient getClient() {
      return Objects.requireNonNull(schema.unwrap(CouchSchema.class)).dbClient;
    }

    public Enumerable<Object> find(List<Map.Entry<String, Class>> fields,
        List<String> ops,
        List<Map.Entry<String, RelFieldCollation.Direction>> sort,
        Long skip) {
      return getTable().find(getClient(), fields, ops, sort, skip);
    }
  }
}
