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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CouchToEnumerableConverter extends ConverterImpl implements EnumerableRel {
  /**
   * Creates a ConverterImpl.
   *
   * @param cluster  planner's cluster
   * @param traitDef the RelTraitDef this converter converts
   * @param traits   the output traits of this converter
   * @param child    child rel (provides input traits)
   */
  protected CouchToEnumerableConverter(
      RelOptCluster cluster,
      @Nullable RelTraitDef traitDef,
      RelTraitSet traits,
      RelNode child) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, child);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CouchToEnumerableConverter(getCluster(), null, traitSet, sole(inputs));
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    return null;
  }
}
