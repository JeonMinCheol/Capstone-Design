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

import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableSet;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CouchProject extends Project implements CouchRel {
  protected CouchProject(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());

    System.out.println("CouchProject.CouchProject");
    assert getConvention() == CouchRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  final boolean isSelectAll(String name) {
    return "_MAP".equals(name);
  }

  // Project Rule 반환
  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new CouchProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  // rex translator를 통해 변환된 데이터를 여기서 판단해서 implementor에 집어넣음
  // 이후 find 함수에서 implementor에 있는 데이터들을 가져다 최종적으로 쿼리로 변경
  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    final List<String> inFields =
        CouchRules.couchFieldNames(getInput().getRowType());

    final CouchRules.RexToCouchTranslator translator =
        new CouchRules.RexToCouchTranslator(
            (JavaTypeFactory) getCluster().getTypeFactory(), inFields);

    final List<String> fields = new ArrayList<>();

    // registers wherever "select *" is present
    boolean hasSelectStar = false;

    // left : RexNode (expr)
    // right : String (Name)
    for (Pair<RexNode, String> pair : getNamedProjects()) {
      final String name = pair.right;
      final String expr = pair.left.accept(translator); // RexToElasticsearchTranslator로 RexNode 변환

      // "select _MAP" present?
      hasSelectStar |= isSelectAll(name);

      // ex) _MAP['_id']
      if (CouchRules.isItem(pair.left)) {
        fields.add(CouchRules.quote(expr));
      } else if (expr.equals(name)) {
        fields.add(name);
      }
    }

    if (hasSelectStar) {
      // means select * from elastic
      // this does not yet cover select *, _MAP['foo'], _MAP['bar'][0] from elastic
      return;
    }

    final StringBuilder query = new StringBuilder();
    List<String> newList = new ArrayList<>(fields);

    final String findString = String.join(", ", newList);
    query.append("\"fields\": [").append(findString).append("]");
    implementor.add(String.valueOf(query));
  }
}
