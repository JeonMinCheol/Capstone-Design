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
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface CouchRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("COUCH", CouchRel.class);

  class Implementor {
    final List<String> list = new ArrayList<>();
    final List<Map.Entry<String, RelFieldCollation.Direction>> sort = new ArrayList<>();
    int skip = 0;
    final RexBuilder rexBuilder;
    RelOptTable table;
    CouchTable couchTable;

    public Implementor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    void addSort(String field, RelFieldCollation.Direction direction) {
      Objects.requireNonNull(field, "field");
      sort.add(new Pair<>(field, direction));
    }

    public void add(String findOp) {list.add(findOp);}

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((CouchRel) input).implement(this);
    }
  }
}
