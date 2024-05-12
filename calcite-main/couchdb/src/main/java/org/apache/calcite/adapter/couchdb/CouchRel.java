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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.util.*;

public interface CouchRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("COUCH", CouchRel.class);

  class Implementor {
    public Implementor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    private RexBuilder rexBuilder;
    private String filterString;
    private String projectString;
    private String sort;
    //Sorting clauses.

    /**
     * EXPR$0과 item간 mapping
     */
    static Map<String, String> expressionItemMap = new LinkedHashMap<>();

    // Starting index
    private int skip = 0;
    RelOptTable table;
    CouchTable couchTable;

    // list에 find에 사용할 Operation 추가
    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((CouchRel) input).implement(this);
    }
    public static void addExpressionItemMapping(String name, String expr) { expressionItemMap.put(name, expr); }
    public static Map<String, String> getExpressionItemMapping() { return expressionItemMap; }
    public String getProjectString() { return projectString; }
    public void setProjectString(String projectString) { this.projectString = projectString; }
    public RexBuilder getRexBuilder() { return rexBuilder; }
    public void setFilterString(String filterString) {this.filterString = filterString;}
    public String getFilterString() { return filterString; }
    public String getSortString() { return sort;}
    public void setSortString(String sort) { this.sort = sort; }
    public int getSkip() { return skip; }
    public void setSkip(int skip) { this.skip = skip; }
  }
}
