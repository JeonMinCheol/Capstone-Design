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
    String dbName = properties.getDbName();
    builder.put(dbName, new CouchTable(this, jsonParser, dbName));
    return builder.build();
  }

  public CouchDbClient getDbClient() {
    return dbClient;
  }
}
