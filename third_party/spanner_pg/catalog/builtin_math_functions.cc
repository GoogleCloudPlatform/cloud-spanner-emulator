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

#include "zetasql/public/types/type.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"

namespace postgres_translator {

void AddArithmeticFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();

  functions.push_back(
      {"int8pl",
       "$add",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8pl",
       "$add",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8mi",
       "$subtract",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8mi",
       "$subtract",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8div",
       "$divide",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8mul",
       "$multiply",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8mul",
       "$multiply",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8um",
       "$unary_minus",
       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8um",
       "$unary_minus",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
}

void AddBitwiseFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();

  functions.push_back(
      {"int8not",
       "$bitwise_not",
       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8or",
       "$bitwise_or",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8xor",
       "$bitwise_xor",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8and",
       "$bitwise_and",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8shl",
       "$bitwise_left_shift",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8shr",
       "$bitwise_right_shift",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
}

void AddApproxFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddStatisticalFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddAnalyticFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddNumericFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();

  functions.push_back(
      {"abs",
       "abs",
       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8abs",
       "abs",
       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8abs",
       "abs",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"sign",
       "sign",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"round",
       "round",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"trunc",
       "trunc",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"ceil",
       "ceil",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"ceiling",
       "ceil",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"floor",
       "floor",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"mod",
       "mod",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8mod",
       "mod",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8div",
       "div",
       {{{gsql_int64, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"pow",
       "pow",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"dpow",
       "pow",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"power",
       "pow",
       {{{gsql_double, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"sqrt",
       "sqrt",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"dsqrt",
       "sqrt",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"exp",
       "exp",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"dexp",
       "exp",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"ln", "ln", {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"dlog1",
       "ln",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"log",
       "log10",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"dlog10",
       "log10",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
}

void AddTrigonometricFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();

  functions.push_back(
      {"cos",
       "cos",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"acos",
       "acos",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"sin",
       "sin",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"asin",
       "asin",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"tan",
       "tan",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"atan",
       "atan",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"atan2",
                       "atan2",
                       {{{gsql_double,
                          {gsql_double, gsql_double},
                          /*context_ptr=*/nullptr}}}});

  /* TODO
   * Postgres 11 does not include hyperbolic functions
   * These were added in Postgres 12
   * Uncomment and add functions after upgrade to postgres 13
   */

  //   functions.push_back(
  //       {"cosh",
  //        "cosh",
  //        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});

  //   functions.push_back(
  //       {"acosh",
  //        "acosh",
  //        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});

  //   functions.push_back(
  //       {"sinh",
  //        "sinh",
  //        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});

  //   functions.push_back(
  //       {"asinh",
  //        "asinh",
  //        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
  //   functions.push_back(
  //       {"tanh",
  //        "tanh",
  //        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});

  //   functions.push_back(
  //       {"atanh",
  //        "atanh",
  //        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}}});
}

}  // namespace postgres_translator
