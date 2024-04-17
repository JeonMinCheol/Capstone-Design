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

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CouchTableScan extends TableScan implements CouchRel{
  final CouchTable couchTable;
  final RelDataType projectRowType;

  public CouchTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, CouchTable couchTable, RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.couchTable = couchTable;
    this.projectRowType = projectRowType;

    assert couchTable != null;
    assert getConvention() == CouchRel.CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // scans with a small project list are cheaper
    final float f =
        projectRowType == null ? 1f :
          projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(CouchToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : CouchRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.couchTable = couchTable;
    implementor.table = table;
  }
}
