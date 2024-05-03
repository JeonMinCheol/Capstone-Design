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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import com.google.common.collect.ImmutableMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.io.IOException;
import java.util.Map;


public class CouchSchema extends AbstractSchema {
  protected final CouchDbClient dbClient;
  protected CouchDbProperties properties;
  protected final Map<String, Object> infoMap;
  protected final JSONParser jsonParser;

  public CouchSchema(Map<String, Object> infoMap, int port) {
    super();
    this.infoMap = infoMap;
    this.jsonParser = new JSONParser();

    this.properties = new CouchDbProperties();
    this.properties.setConnectionTimeout(0);
    this.properties.setProtocol((String) infoMap.get("protocol"));
    this.properties.setUsername((String) infoMap.get("username"));
    this.properties.setPassword((String) infoMap.get("password"));
    this.properties.setPort(port);
    this.properties.setHost((String) infoMap.get("host"));
    this.properties.setDbName((String) infoMap.get("database"));

    this.dbClient = new CouchDbClient(properties);
  }

  // couchDB API를 통해 document 목록을 불러와서 테이블을 생성
  @Override
  protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // Database에 위치한 docs들 정보를 가져옴
    HttpGet uri = new HttpGet(dbClient.getDBUri()+"/_all_docs");
    CloseableHttpResponse response = (CloseableHttpResponse) dbClient.executeRequest(uri);

    try {
      String json = EntityUtils.toString(response.getEntity(), "UTF-8");
      JSONArray rows = (JSONArray)((JSONObject) jsonParser.parse(json)).get("rows");

      for (int i = 0; i < rows.size(); i++) {
        JSONObject parse = (JSONObject) rows.get(i);
        String documentId = (String) parse.get("id");

        builder.put(documentId, new CouchTable(this, null, jsonParser, documentId));
      }
    } catch (IOException | ParseException e) {
        throw new RuntimeException(e);
    }

    return builder.build();
  }

  public CouchDbClient getDbClient() {
    return dbClient;
  }
}
