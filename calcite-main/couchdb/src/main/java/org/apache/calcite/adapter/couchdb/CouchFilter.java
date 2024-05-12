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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.sun.java.accessibility.util.Translator;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.*;

import org.apache.calcite.util.JsonBuilder;

import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class CouchFilter extends Filter implements CouchRel {
  protected CouchFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == CouchRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  // Filter Rule 반환
  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new CouchFilter(getCluster(), traitSet, input, condition);
  }

  // TODO
  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Translator translator = new Translator(implementor.getRexBuilder(), CouchRules.couchFieldNames(getRowType()));
    String selector = translator.translateSelect(condition);
    implementor.setFilterString(selector);
  }

  static class Translator {
    final JsonBuilder builder = new JsonBuilder();
    private final RexBuilder rexBuilder;
    private final List<String> fieldNames;

    final Multimap<String, Pair<String, RexLiteral>> multimap =
        HashMultimap.create();
    final Map<String, RexLiteral> eqMap =
        new LinkedHashMap<>();

    public Translator(RexBuilder rexBuilder, List<String> fieldNames) {
      this.rexBuilder = rexBuilder;
      this.fieldNames = fieldNames;
    }

    public String translateSelect(RexNode condition) {
      return builder.toJsonString(translateOr(condition));
    }

    private Object translateOr(RexNode condition) {
      final RexNode condition2 =
          RexUtil.expandSearch(rexBuilder, null, condition);

      List<Object> list = new ArrayList<>();
      for(RexNode node : RelOptUtil.disjunctions(condition2)) {
        list.add(translateAnd(node));
      }
      switch (list.size()) {
      case 1:
        return list.get(0);
      default:
        Map<String, Object> map = builder.map();
        map.put("$or", list);
        return map;
      }
    }

    /** Translates a condition that may be an AND of other conditions. Gathers
     * together conditions that apply to the same field. */
    private Map<String, Object> translateAnd(RexNode node0) {
      eqMap.clear();
      multimap.clear();
      for (RexNode node : RelOptUtil.conjunctions(node0)) {
        translateMatch2(node);
      }
      Map<String, Object> map = builder.map();
      for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
        multimap.removeAll(entry.getKey());
        map.put(entry.getKey(), literalValue(entry.getValue()));
      }
      for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
          : multimap.asMap().entrySet()) {
        Map<String, Object> map2 = builder.map();
        for (Pair<String, RexLiteral> s : entry.getValue()) {
          addPredicate(map2, s.left, literalValue(s.right));
        }
        map.put(entry.getKey(), map2);
      }
      return map;
    }

    private static void addPredicate(Map<String, Object> map, String op, Object v) {
      if (map.containsKey(op) && stronger(op, map.get(op), v)) {
        return;
      }
      map.put(op, v);
    }

    /** Returns whether {@code v0} is a stronger value for operator {@code key}
     * than {@code v1}.
     *
     * <p>For example, {@code stronger("$lt", 100, 200)} returns true, because
     * "&lt; 100" is a more powerful condition than "&lt; 200".
     */
    private static boolean stronger(String key, Object v0, Object v1) {
      if (key.equals("$lt") || key.equals("$lte")) {
        if (v0 instanceof Number && v1 instanceof Number) {
          return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
        }
        if (v0 instanceof String && v1 instanceof String) {
          return v0.toString().compareTo(v1.toString()) < 0;
        }
      }
      if (key.equals("$gt") || key.equals("$gte")) {
        return stronger("$lt", v1, v0);
      }
      return false;
    }

    private static Object literalValue(RexLiteral literal) {
      return literal.getValue2();
    }

    private Void translateLike(RexCall call) {

      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);

      boolean b = translateLike2(left ,right);

      if (b) {
        return null;
      }

      throw new AssertionError("cannot translate op call" + call);
    }

    private Void translateMatch2(RexNode node) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary(null, null, (RexCall) node);
      case LESS_THAN:
        return translateBinary("$lt", "$gt", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("$lte", "$gte", (RexCall) node);
      case NOT_EQUALS:
        return translateBinary("$ne", "$ne", (RexCall) node);
      case GREATER_THAN:
        return translateBinary("$gt", "$lt", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary("$gte", "$lte", (RexCall) node);
      case LIKE:
        return translateLike((RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
    private Void translateBinary(String op, String rop, RexCall call) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      boolean b = translateBinary2(op, left, right);
      if (b) {
        return null;
      }
      b = translateBinary2(rop, right, left);
      if (b) {
        return null;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /** Translates a call to a binary operator. Returns whether successful. */
    private boolean translateBinary2(String op, RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return false;
      }
      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        translateOp2(op, name, rightLiteral);
        return true;
      case CAST:
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      case ITEM:
        String itemName = CouchRules.isItem2((RexCall) left);
        if (itemName != null) {
          translateOp2(op, itemName, rightLiteral);
          return true;
        }
        // fall through
      default:
        return false;
      }
    }

    private void translateOp2(String op, String name, RexLiteral right) {
      if (op == null) {
        // E.g.: {deptno: 100}
        eqMap.put(name, right);
      } else {
        // E.g. {deptno: {$lt: 100}}
        // which may later be combined with other conditions:
        // E.g. {deptno: [$lt: 100, $gt: 50]}
        multimap.put(name, Pair.of(op, right));
      }
    }

    private boolean translateLike2(RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return false;
      }

      final RexLiteral rightLiteral = (RexLiteral) right;

      String likePattern = rightLiteral.getValue2().toString();
      System.out.println(likePattern);

      String regexPatternString = likePattern.replace("*", "");
      if(regexPatternString.charAt(0) == '%' && regexPatternString.charAt(regexPatternString.length()-1) == '%') {
        regexPatternString = regexPatternString.substring(1, regexPatternString.length()-1);
      } else if (regexPatternString.charAt(0) == '%') {
        regexPatternString = regexPatternString.substring(1) + "$";
      } else if (regexPatternString.charAt(regexPatternString.length()-1) == '%') {
        regexPatternString = "^" + regexPatternString.substring(0, regexPatternString.length()-1);
      }

      RexLiteral regexPattern = rexBuilder.makeLiteral(regexPatternString);

      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        translateOp2("$regex", name, regexPattern);
        return true;
      case CAST:
        return translateLike2(((RexCall) left).operands.get(0), right);
      case ITEM:
        String itemName = CouchRules.isItem2((RexCall) left);
        if (itemName != null) {
          translateOp2("$regex", itemName, regexPattern);
          return true;
        }
      default:
        return false;
      }
    }
  }
}
