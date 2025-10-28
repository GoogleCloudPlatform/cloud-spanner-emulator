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

#include "third_party/spanner_pg/catalog/builtin_expression_functions.h"

#include <string>

#include "absl/container/flat_hash_map.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/postgres_includes/all.h"  // NOLINT(misc-include-cleaner)

namespace postgres_translator {

namespace {

void AddArrayFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true,
                        /*use_or=*/true,
                        /*comparator_type=*/"="),
                    "$in_array"});

  // We use safe_array_at_ordinal to match two PostgreSQL behaviors: 1-based
  // indexing (ordinal), and NULL return values for out-of-bounds accesses
  // (safe).
  functions.insert(
      {PostgresExprIdentifier::SubscriptingRef(/*is_array_slice=*/false),
       "$safe_array_at_ordinal"});

  functions.insert(
      {PostgresExprIdentifier::SubscriptingRef(/*is_array_slice=*/true),
       "pg.array_slice"});

  // IMPORTANT NOTE: This mapping only applies when the array contains non-Const
  // element expressions (matches ZetaSQL behavior). Arrays of all Const
  // elements are transformed to ResolvedLiterals.
  // make_array is the general-form array constructor, used in cases where the
  // array constant has non-const element expressions (Var, Expr, etc.).
  functions.insert({PostgresExprIdentifier::Expr(T_ArrayExpr), "$make_array"});

  // We do not have a mapping for pg.array_all_not_equal because this is
  // equivalent to `NOT IN` and implemented as such.
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true,
                        /*use_or=*/false,
                        /*comparator_type=*/"="),
                    "pg.array_all_equal"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true,
                        /*use_or=*/false,
                        /*comparator_type=*/">"),
                    "pg.array_all_greater"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true,
                        /*use_or=*/false,
                        /*comparator_type=*/">="),
                    "pg.array_all_greater_equal"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true,
                        /*use_or=*/false,
                        /*comparator_type=*/"<"),
                    "pg.array_all_less"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/true,
                        /*use_or=*/false,
                        /*comparator_type=*/"<="),
                    "pg.array_all_less_equal"});
}

void AddBoolFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::BoolExpr(AND_EXPR), "$and"});
  functions.insert(
      {PostgresExprIdentifier::BooleanTest(IS_FALSE), "$is_false"});
  functions.insert({PostgresExprIdentifier::BooleanTest(IS_TRUE), "$is_true"});
  functions.insert({PostgresExprIdentifier::BoolExpr(NOT_EXPR), "$not"});
  functions.insert({PostgresExprIdentifier::BoolExpr(OR_EXPR), "$or"});
}

void AddComparisonFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert(
      {PostgresExprIdentifier::CaseExpr(/*case_has_testexpr=*/false),
       "$case_no_value"});
  functions.insert(
      {PostgresExprIdentifier::CaseExpr(/*case_has_testexpr=*/true),
       "$case_with_value"});
  functions.insert({PostgresExprIdentifier::ScalarArrayOpExpr(
                        /*array_op_arg_is_array=*/false,
                        /*use_or=*/true,
                        /*comparator_type=*/"="),
                    "$in"});
}

void AddLeastGreatestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert(
      {PostgresExprIdentifier::MinMaxExpr(IS_GREATEST), "pg.greatest"});
  functions.insert({PostgresExprIdentifier::MinMaxExpr(IS_LEAST), "pg.least"});
}

void AddNullTestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert({PostgresExprIdentifier::NullTest(IS_NULL), "$is_null"});
  functions.insert({PostgresExprIdentifier::Expr(T_CoalesceExpr), "coalesce"});
  functions.insert({PostgresExprIdentifier::Expr(T_NullIfExpr), "nullif"});
}

void AddTimeFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  functions.insert(
      {PostgresExprIdentifier::SQLValueFunction(SVFOP_CURRENT_DATE),
       "current_date"});
  functions.insert(
      {PostgresExprIdentifier::SQLValueFunction(SVFOP_CURRENT_TIMESTAMP),
       "current_timestamp"});
}
}  // namespace

void AddExpressionFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  AddArrayFunctions(functions);
  AddBoolFunctions(functions);
  AddComparisonFunctions(functions);
  AddLeastGreatestFunctions(functions);
  AddNullTestFunctions(functions);
  AddTimeFunctions(functions);
}

}  // namespace postgres_translator
