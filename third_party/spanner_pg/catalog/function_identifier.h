//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#ifndef CATALOG_FUNCTION_IDENTIFIERS_H_
#define CATALOG_FUNCTION_IDENTIFIERS_H_

#include <string>

#include "zetasql/base/logging.h"
#include "absl/container/flat_hash_map.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

class PostgresExprIdentifier {
 public:
  // Constructor for non-specialized exprs that can be identified by  just the
  // NodeTag.
  static PostgresExprIdentifier Expr(NodeTag node_tag) {
    ABSL_CHECK_NE(node_tag, T_BoolExpr);
    ABSL_CHECK_NE(node_tag, T_NullTest);
    ABSL_CHECK_NE(node_tag, T_MinMaxExpr);
    ABSL_CHECK_NE(node_tag, T_BooleanTest);
    ABSL_CHECK_NE(node_tag, T_CaseExpr);
    ABSL_CHECK_NE(node_tag, T_SQLValueFunction);
    ABSL_CHECK_NE(node_tag, T_ScalarArrayOpExpr);
    return PostgresExprIdentifier(node_tag);
  }

  // Special constructor for BoolExpr.
  static PostgresExprIdentifier BoolExpr(BoolExprType bool_type) {
    return PostgresExprIdentifier(T_BoolExpr).SetBoolExprType(bool_type);
  }

  // Special constructor for ScalarArrayOpExpr.
  // ScalarArrayOpExpr represents ANY/SOME/ALL(<array>) and also IN(values).
  // PG query tree does not distinguish between both types of queries.
  //
  // However, GSQL distinguishes between both cases. In GSQL, `$in_array`
  // gets invoked for `<value> IN <array>`, which is the GSQL equivalent of
  // PG's ANY/SOME(<array>) while `$in` gets invoked for
  // `<value> IN (<comma separated list>)`.
  static PostgresExprIdentifier ScalarArrayOpExpr(bool array_op_arg_is_array) {
    return PostgresExprIdentifier(T_ScalarArrayOpExpr)
        .SetArrayOpArgIsArray(array_op_arg_is_array);
  }

  // Special constructor for NullTest.
  static PostgresExprIdentifier NullTest(NullTestType null_test_type) {
    return PostgresExprIdentifier(T_NullTest).SetNullTestType(null_test_type);
  }

  // Special constructor for MinMaxExpr.
  static PostgresExprIdentifier MinMaxExpr(MinMaxOp min_max_op) {
    return PostgresExprIdentifier(T_MinMaxExpr).SetMinMaxOp(min_max_op);
  }
  // Special constructor for BooleanTest.
  static PostgresExprIdentifier BooleanTest(BoolTestType boolean_test_type) {
    return PostgresExprIdentifier(T_BooleanTest)
        .SetBooleanTestType(boolean_test_type);
  }

  // Special constructor for CaseExpr.
  static PostgresExprIdentifier CaseExpr(bool case_has_testexpr) {
    return PostgresExprIdentifier(T_CaseExpr)
        .SetCaseHasTestExpr(case_has_testexpr);
  }

  static PostgresExprIdentifier SQLValueFunction(
      SQLValueFunctionOp function_op) {
    return PostgresExprIdentifier(T_SQLValueFunction)
        .SetSQLValueFunctionOp(function_op);
  }

  bool operator==(const PostgresExprIdentifier& other) const {
    return node_tag_ == other.node_tag() &&
           bool_expr_type_ == other.bool_expr_type() &&
           null_test_type_ == other.null_test_type() &&
           min_max_op_ == other.min_max_op() &&
           boolean_test_type_ == other.bool_test_type() &&
           case_has_testexpr_ == other.case_has_testexpr() &&
           sql_value_function_op_ == other.sql_value_function_op() &&
           array_op_arg_is_array_ == other.array_op_arg_is_array();
  }

  template <typename H>
  friend H AbslHashValue(H state, const PostgresExprIdentifier& expr_id) {
    return H::combine(
        std::move(state), expr_id.node_tag(), expr_id.bool_expr_type(),
        expr_id.null_test_type(), expr_id.min_max_op(),
        expr_id.bool_test_type(), expr_id.case_has_testexpr(),
        expr_id.sql_value_function_op(), expr_id.array_op_arg_is_array());
  }

  NodeTag node_tag() const { return node_tag_; }

  BoolExprType bool_expr_type() const { return bool_expr_type_; }

  NullTestType null_test_type() const { return null_test_type_; }

  MinMaxOp min_max_op() const { return min_max_op_; }

  BoolTestType bool_test_type() const { return boolean_test_type_; }

  bool case_has_testexpr() const { return case_has_testexpr_; }

  bool array_op_arg_is_array() const { return array_op_arg_is_array_; }

  SQLValueFunctionOp sql_value_function_op() const {
    return sql_value_function_op_;
  }

 private:
  PostgresExprIdentifier(NodeTag node_tag)
      : node_tag_(node_tag),
        bool_expr_type_(AND_EXPR),
        null_test_type_(IS_NULL),
        min_max_op_(IS_GREATEST),
        boolean_test_type_(IS_FALSE),
        case_has_testexpr_(false),
        sql_value_function_op_(SVFOP_CURRENT_DATE),
        array_op_arg_is_array_(false) {}

  PostgresExprIdentifier& SetBoolExprType(BoolExprType bool_expr_type) {
    bool_expr_type_ = bool_expr_type;
    return *this;
  }

  PostgresExprIdentifier& SetNullTestType(NullTestType null_test_type) {
    null_test_type_ = null_test_type;
    return *this;
  }

  PostgresExprIdentifier& SetMinMaxOp(MinMaxOp min_max_op) {
    min_max_op_ = min_max_op;
    return *this;
  }

  PostgresExprIdentifier& SetArrayOpArgIsArray(bool array_op_arg_is_array) {
    array_op_arg_is_array_ = array_op_arg_is_array;
    return *this;
  }

  PostgresExprIdentifier& SetBooleanTestType(BoolTestType boolean_test_type) {
    boolean_test_type_ = boolean_test_type;
    return *this;
  }

  PostgresExprIdentifier& SetCaseHasTestExpr(bool case_has_testexpr) {
    case_has_testexpr_ = case_has_testexpr;
    return *this;
  }

  PostgresExprIdentifier& SetSQLValueFunctionOp(
      SQLValueFunctionOp function_op) {
    sql_value_function_op_ = function_op;
    return *this;
  }

  // All Exprs have a NodeTag.
  NodeTag node_tag_;

  // Specialized fields.
  BoolExprType bool_expr_type_;
  NullTestType null_test_type_;
  MinMaxOp min_max_op_;
  BoolTestType boolean_test_type_;
  bool case_has_testexpr_;
  SQLValueFunctionOp sql_value_function_op_;
  bool array_op_arg_is_array_;
};

}  // namespace postgres_translator
#endif  // CATALOG_FUNCTION_IDENTIFIERS_H_
