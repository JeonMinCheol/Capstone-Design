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

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.trace.CalciteLogger;

import org.apache.calcite.util.trace.CalciteTrace;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.AbstractList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchRules {
  public CouchRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final RelOptRule[] RULES = {
      CouchProjectRule.INSTANCE
  };

  static String isItem(RexCall call) {
    if (call.getOperator() != SqlStdOperatorTable.ITEM) {
      return null;
    }
    final RexNode op0 = call.operands.get(0);
    final RexNode op1 = call.operands.get(1);
    if (op0 instanceof RexInputRef
        && ((RexInputRef) op0).getIndex() == 0
        && op1 instanceof RexLiteral
        && ((RexLiteral) op1).getValue2() instanceof String) {
      return (String) ((RexLiteral) op1).getValue2();
    }
    return null;
  }

  static List<String> couchFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override public String get(int index) {
            final String name = rowType.getFieldList().get(index).getName();
            return name.startsWith("$") ? "_" + name.substring(2) : name;
          }

          @Override public int size() {
            return rowType.getFieldCount();
          }
        },
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  static String maybeQuote(String s) {
    if (!needsQuote(s)) {
      return s;
    }
    return quote(s);
  }

  static String quote(String s) {
    return "\"" + s + "\"";
  }

  private static boolean needsQuote(String s) {
    for (int i = 0, n = s.length(); i < n; i++) {
      char c = s.charAt(i);
      if (!Character.isJavaIdentifierPart(c)
          || c == '$') {
        return true;
      }
    }
    return false;
  }

  static String stripQuotes(String s) {
    return s.length() > 1 && s.startsWith("\"") && s.endsWith("\"")
        ? s.substring(1, s.length() - 1) : s;
  }

  static class RexToCouchTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    private static final Map<SqlOperator, String> COUCH_OPERATORS =
        new HashMap<>();

    static {
      // Arithmetic
      COUCH_OPERATORS.put(SqlStdOperatorTable.MOD, "$mod");
      // Boolean
      COUCH_OPERATORS.put(SqlStdOperatorTable.AND, "$and");
      COUCH_OPERATORS.put(SqlStdOperatorTable.OR, "$or");
      COUCH_OPERATORS.put(SqlStdOperatorTable.NOT, "$not");
      // Comparison
      COUCH_OPERATORS.put(SqlStdOperatorTable.EQUALS, "$eq");
      COUCH_OPERATORS.put(SqlStdOperatorTable.NOT_EQUALS, "$ne");
      COUCH_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN, "$gt");
      COUCH_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "$gte");
      COUCH_OPERATORS.put(SqlStdOperatorTable.LESS_THAN, "$lt");
      COUCH_OPERATORS.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "$lte");
      COUCH_OPERATORS.put(SqlStdOperatorTable.IN, "$in");
      COUCH_OPERATORS.put(SqlStdOperatorTable.ALL_EQ, "$all");
    }

    protected RexToCouchTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override
    public String visitLiteral(RexLiteral literal) {
      if (literal.getValue() == null) {
        return "null";
      }
      return "{"
          + RexToLixTranslator.translateLiteral(literal, literal.getType(),
          typeFactory, RexImpTable.NullAs.NOT_POSSIBLE)
          + "}";
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return maybeQuote(
          "$" + inFields.get(inputRef.getIndex()));
    }
  }
  abstract static class CouchConverterRule extends ConverterRule {
    public CouchConverterRule(Config config) {
      super(config);
    }
  }

  private static class CouchProjectRule extends CouchConverterRule {
    static final CouchProjectRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            CouchRel.CONVENTION, "CouchProjectRule")
        .withRuleFactory(CouchProjectRule::new)
        .toRule(CouchProjectRule.class);

    public CouchProjectRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      return project.getVariablesSet().isEmpty();
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new CouchProject(project.getCluster(), traitSet, convert(project.getInput(), out),
          project.getProjects(), project.getRowType());
    }
  }
}
