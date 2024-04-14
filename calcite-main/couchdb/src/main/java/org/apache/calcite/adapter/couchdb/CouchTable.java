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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Page;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CouchTable extends AbstractQueryableTable implements TranslatableTable {
  private final String documentId;
  private final JSONParser jsonParser;
  CouchTable(String documentId, JSONParser jsonParser){
      super(Object[].class);
      this.documentId = documentId;
      this.jsonParser = jsonParser;
  }

  @Override
  public String toString() {return "CouchTable {" + documentId + '}';}

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new CouchQueryable<>(queryProvider, schema, this, tableName);
  }

  // table의 attribute 설정
  // MAP이라는 attirbute 하나만 생성해서 때려박음
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true
            )
        );

    return typeFactory.builder().add(documentId, mapType).build();
  }

  // TODO
  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CouchTableScan(cluster, cluster.traitSet(), relOptTable);
  }

  // table(document)내의 row를 가져옴
  private Enumerable<Object> find(CouchDbClient dbClient, String documentId) {
    String tableUri = dbClient.getDBUri()+ "/" + documentId;

    // document 불러오는 코드
    HttpGet docs = new HttpGet(tableUri);
    HttpEntity entity = dbClient.executeRequest(docs).getEntity();
    List<Object> list = new ArrayList<>();

    try{
      String res  = EntityUtils.toString(entity,"UTF-8");
      JSONObject rows = (JSONObject) jsonParser.parse(res);

      int row = 0;

      while(true) {
        try{
          Object o = rows.get(String.valueOf(row));
          list.add(o);

          row++;
        } catch (Exception e) {
          break;
        }
      }
    }catch (Exception e){
      throw new RuntimeException(e);
    }

    return new AbstractEnumerable<Object>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new CouchEnumerator(list);
      }
    };
  }

  private class CouchQueryable<T> extends AbstractTableQueryable<T> {
    public CouchQueryable(QueryProvider queryProvider, SchemaPlus schema, CouchTable couchTable,
        String tableName) {
      super(queryProvider, schema, couchTable, tableName);
    }

    // 현재 테이블의 enumerator를 반환
    @Override
    public Enumerator<T> enumerator() {
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().find(getClient(), tableName);

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
  }
}
