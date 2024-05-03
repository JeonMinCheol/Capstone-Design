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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

public class CouchProject extends Project implements CouchRel {
  protected CouchProject(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    assert getConvention() == CouchRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  // Project Rule 반환
  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new CouchProject(getCluster(), traitSet, input, projects, rowType);
  }


  // TODO : project에서 name은 field를 의미하는 듯
  // rex translator를 통해 변환된 데이터를 여기서 판단해서 implementor에 집어넣음
  // 이후 find 함수에서 implementor에 있는 데이터들을 가져다 최종적으로 쿼리로 변경
  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());


  }
}
