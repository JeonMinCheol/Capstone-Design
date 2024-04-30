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

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.apache.calcite.rel.type.RelDataType;

import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
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

  // query에 필요한 정보들을 전달
  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder list = new BlockBuilder();
    final CouchRel.Implementor CouchImplementor = new CouchRel.Implementor(getCluster().getRexBuilder());
    CouchImplementor.visitChild(0, getInput());
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));


    final Expression fields =
        list.append("fields",
            constantArrayList(
                Pair.zip(CouchRules.couchFieldNames(rowType),
                    new AbstractList<Class>() {
                      @Override public Class get(int index) {
                        return physType.fieldClass(index);
                      }

                      @Override public int size() {
                        return rowType.getFieldCount();
                      }
                    }),
                Pair.class));

    final Expression table = list.append("table", CouchImplementor.table.getExpression(CouchTable.CouchQueryable.class));
    final Expression sort = list.append("sort", Expressions.constant(CouchImplementor, Pair.class));
    final Expression skip = list.append("skip", Expressions.constant(CouchImplementor.skip));
    final Expression ops = list.append("ops", Expressions.constant(CouchImplementor.list));

    Expression enumerable =
        list.append("enumerable",
            Expressions.call(table,
                CouchMethod.COUCH_QUERYABLE_FIND.method, fields, ops, sort, skip));

    list.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  private static <T> MethodCallExpression constantArrayList(List<T> values,
      Class clazz) {
    return Expressions.call(
        BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /** E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
  private static <T> List<Expression> constantList(List<T> values) {
    return Util.transform(values, Expressions::constant);
  }
}
