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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
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

  // CouchRule들의 목록
  public static final RelOptRule[] RULES = {
      CouchProjectRule.INSTANCE,
      CouchFilterRule.INSTANCE
  };

  /**
   * Returns 'string' if it is a call to item['string'], null otherwise.
   *
   * @param call current relational expression
   * @return literal value
   */
  public static String isItemCall(RexCall call) {

    // item = For example, myArray[3], "myMap['foo']", myStruct[2] or myStruct['fieldName']
    if (call.getOperator() != SqlStdOperatorTable.ITEM) {
      return null;
    }
    final RexNode op0 = call.getOperands().get(0); // operands : 피연산자
    final RexNode op1 = call.getOperands().get(1);

    //         RexCall(=)           : operator
    //         /       \
    //        /         \
    // InputRef($1) Literal("John") : operands

    if (op0 instanceof RexInputRef
        && ((RexInputRef) op0).getIndex() == 0
        && op1 instanceof RexLiteral
        && ((RexLiteral) op1).getValue2() instanceof String) {
      return (String) ((RexLiteral) op1).getValue2();
    }
    return null;
  }

  /**
   * Checks if current node represents item access as in {@code _MAP['foo']} or
   * {@code cast(_MAP['foo'] as integer)}.
   *
   * @return whether expression is item
   */
  static boolean isItem(RexNode node) {
    final Boolean result = node.accept(new RexVisitorImpl<Boolean>(false) {
      @Override public Boolean visitCall(final RexCall call) {
        return isItemCall(uncast(call)) != null;
      }
    });

    return Boolean.TRUE.equals(result);
  }

  /**
   * Unwraps cast expressions from current call. {@code cast(cast(expr))} becomes {@code expr}.
   */
  private static RexCall uncast(RexCall maybeCast) {
    if (maybeCast.getKind() == SqlKind.CAST && maybeCast.getOperands().get(0) instanceof RexCall) {
      return uncast((RexCall) maybeCast.getOperands().get(0));
    }

    // not a cast
    return maybeCast;
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

  public static String isItem2(RexCall call) {
    if (call.getOperator() != SqlStdOperatorTable.ITEM) return null;

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

  // TODO : RexNode를 쿼리로 변환하는 메서드를 담는 클래스
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
      COUCH_OPERATORS.put(SqlStdOperatorTable.LIKE, "$regex");
    }

    protected RexToCouchTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    // literal 방문 시 literal: 마킹해서 Rule에서 확인 후 변환
    @Override
    public String visitLiteral(RexLiteral literal) {
      if (literal.getValue() == null) {
        return "null";
      }
      return RexToLixTranslator.translateLiteral(literal,
          literal.getType(), typeFactory, RexImpTable.NullAs.NOT_POSSIBLE).toString();
    }

    // inputRef 방문 시 $index로 변환
    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return maybeQuote(
          "$" + inFields.get(inputRef.getIndex()));
    }

    // TODO : 바꾸기
    @Override
    public String visitCall(RexCall call) {
      final String name = isItemCall(call);
      if (name != null) {
        return name;
      }

      final List<String> strings = visitList(call.operands);

      if (call.getKind() == SqlKind.CAST) {
        return call.getOperands().get(0).accept(this);
      }

      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op1 = call.getOperands().get(1);
        if (op1 instanceof RexLiteral && op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
          return stripQuotes(strings.get(0)) + "[" + ((RexLiteral) op1).getValue2() + "]";
        }
      }
      throw new IllegalArgumentException("Translation of " + call
          + " is not supported by CouchhProject");
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

    protected CouchProjectRule(Config config) {
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
      return new CouchProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(), project.getRowType());
    }
  }

  private static class CouchFilterRule extends CouchConverterRule {
    static final CouchFilterRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalFilter.class, Convention.NONE,
            CouchRel.CONVENTION, "CouchFilterRule")
        .withRuleFactory(CouchFilterRule::new)
        .toRule(CouchFilterRule.class);

    public CouchFilterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new CouchFilter(
          rel.getCluster(),
          traitSet,
          convert(filter.getInput(), out),
          filter.getCondition()
      );
    }
  }
}
