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

#include <string>

#include "absl/container/flat_hash_map.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

// Mappings for "Expression Functions"
//
// Standard PostgreSQL functions are implemented as a generic FuncExpr, with a
// typed argument list, function name, etc. Those functions should be added to
// the system catalog in builtin_xxx_functions.cc files matching their ZetaSQL
// counterpart's categorization.
//
// Another class of function-like expressions is treated differently in
// PostgreSQL. These are represented by some other sybtype of Expr such as
// NullTest, CoalesceExpr, etc. The mapping for this class of "expression
// functions", which typically use generic arguments, is handled here.

namespace postgres_translator {

void AddExprFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::Expr(T_CoalesceExpr), "coalesce"});
  functions.insert(
      {PostgresExprIdentifier::CaseExpr(/*case_has_testexpr=*/false),
       "$case_no_value"});
  functions.insert(
      {PostgresExprIdentifier::CaseExpr(/*case_has_testexpr=*/true),
       "$case_with_value"});
  functions.insert({PostgresExprIdentifier::Expr(T_NullIfExpr), "nullif"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/false),
                    "$in"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true),
                    "$in_array"});
}

void AddBoolExprFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::BoolExpr(AND_EXPR), "$and"});
  functions.insert({PostgresExprIdentifier::BoolExpr(NOT_EXPR), "$not"});
  functions.insert({PostgresExprIdentifier::BoolExpr(OR_EXPR), "$or"});
}

void AddNullTestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::NullTest(IS_NULL), "$is_null"});
}

void AddLeastGreatestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert(
      {PostgresExprIdentifier::MinMaxExpr(IS_GREATEST), "greatest"});
  functions.insert({PostgresExprIdentifier::MinMaxExpr(IS_LEAST), "least"});
}

void AddBooleanTestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::BooleanTest(IS_TRUE), "$is_true"});
  functions.insert(
      {PostgresExprIdentifier::BooleanTest(IS_FALSE), "$is_false"});
}

void AddSQLValueFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert(
      {PostgresExprIdentifier::SQLValueFunction(SVFOP_CURRENT_TIMESTAMP),
       "current_timestamp"});
  functions.insert(
      {PostgresExprIdentifier::SQLValueFunction(SVFOP_CURRENT_DATE),
       "current_date"});
}

void AddArrayAtFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  // We use safe_array_at_ordinal to match two PostgreSQL behaviors: 1-based
  // indexing (ordinal), and NULL return values for out-of-bounds accesses
  // (safe).
  functions.insert({PostgresExprIdentifier::Expr(T_SubscriptingRef),
                    "$safe_array_at_ordinal"});
}

void AddMakeArrayFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  // make_array is the general-form array constructior, used in cases where the
  // array constant has non-const element exressions (Var, Expr, etc.).
  functions.insert({PostgresExprIdentifier::Expr(T_ArrayExpr), "$make_array"});
}

}  // namespace postgres_translator
