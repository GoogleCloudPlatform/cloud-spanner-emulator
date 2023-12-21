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
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/flags/flag.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"

namespace postgres_translator {

// PostgreSQL function signatures that accept or return the numeric type require
// engine-specific support. For Spanner, those signatures are in
// builtin_spanner_functions.cc.
void AddAggregateFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_bool = zetasql::types::BoolType();
  const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
  const zetasql::Type* gsql_date = zetasql::types::DateType();
  const zetasql::Type* gsql_pg_numeric =
      spangres::types::PgNumericMapping()->mapped_type();
  const zetasql::Type* gsql_int64_arr = zetasql::types::Int64ArrayType();
  const zetasql::Type* gsql_string_arr = zetasql::types::StringArrayType();
  const zetasql::Type* gsql_bool_arr = zetasql::types::BoolArrayType();
  const zetasql::Type* gsql_double_arr = zetasql::types::DoubleArrayType();
  const zetasql::Type* gsql_bytes_arr = zetasql::types::BytesArrayType();
  const zetasql::Type* gsql_timestamp_arr =
      zetasql::types::TimestampArrayType();
  const zetasql::Type* gsql_date_arr = zetasql::types::DateArrayType();
  const zetasql::Type* gsql_pg_numeric_array =
      spangres::types::PgNumericArrayMapping()->mapped_type();

  const zetasql::Function::Mode AGGREGATE = zetasql::Function::AGGREGATE;

  functions.push_back({"bit_and",
                       "bit_and",
                       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}},
                       AGGREGATE});
  functions.push_back({"bit_or",
                       "bit_or",
                       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}},
                       AGGREGATE});
  functions.push_back({"bool_and",
                       "logical_and",
                       {{{gsql_bool, {gsql_bool}, /*context_ptr=*/nullptr}}},
                       AGGREGATE});
  functions.push_back({"every",
                       "logical_and",
                       {{{gsql_bool, {gsql_bool}, /*context_ptr=*/nullptr}}},
                       AGGREGATE});
  functions.push_back({"bool_or",
                       "logical_or",
                       {{{gsql_bool, {gsql_bool}, /*context_ptr=*/nullptr}}},
                       AGGREGATE});
    functions.push_back({"count",
                       "count",
                       {{{gsql_int64, {gsql_bool}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_bool_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {gsql_bytes}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_bytes_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_int64_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {gsql_double}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_double_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_string_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {gsql_date}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_date_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_timestamp}, /*context_ptr=*/nullptr}},
                        {{gsql_int64,
                          {gsql_timestamp_arr}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {}, /*context_ptr=*/nullptr},
                         /*has_mapped_function=*/true,
                         /*explicit_mapped_function_name=*/"$count_star"}},
                       AGGREGATE});
  functions.push_back(
      {"min",
       "min",
       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_date, {gsql_date}, /*context_ptr=*/nullptr}},
        {{gsql_timestamp, {gsql_timestamp}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
  functions.push_back(
      {"max",
       "max",
       {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_date, {gsql_date}, /*context_ptr=*/nullptr}},
        {{gsql_timestamp, {gsql_timestamp}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
  functions.push_back(
      {"string_agg",
       "string_agg",
       {{{gsql_bytes, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
  functions.push_back(
      {"sum",
       "sum",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
  functions.push_back(
      {"avg",
       "avg",
       {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
  functions.push_back(
      {"array_agg",
       "array_agg",
       {{{gsql_int64_arr, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_double_arr, {gsql_double}, /*context_ptr=*/nullptr}},
        {{gsql_bool_arr, {gsql_bool}, /*context_ptr=*/nullptr}},
        {{gsql_string_arr, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_bytes_arr, {gsql_bytes}, /*context_ptr=*/nullptr}},
        {{gsql_date_arr, {gsql_date}, /*context_ptr=*/nullptr}},
        {{gsql_pg_numeric_array, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
        {{gsql_timestamp_arr, {gsql_timestamp}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
}

void AddBooleanFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_bool = zetasql::types::BoolType();
  const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
  const zetasql::Type* gsql_date = zetasql::types::DateType();

  functions.push_back(
      {"booleq",
       "$equal",
       {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"byteaeq",
       "$equal",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8eq",
       "$equal",
       {{{gsql_bool, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8eq",
       "$equal",
       {{{gsql_bool, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"texteq",
       "$equal",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_eq",
                       "$equal",
                       {{{gsql_bool,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_eq",
       "$equal",
       {{{gsql_bool, {gsql_date, gsql_date},
          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"boolne",
       "$not_equal",
       {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"byteane",
       "$not_equal",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8ne",
       "$not_equal",
       {{{gsql_bool, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8ne",
       "$not_equal",
       {{{gsql_bool, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"textne",
       "$not_equal",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_ne",
                       "$not_equal",
                       {{{gsql_bool,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_ne",
       "$not_equal",
       {{{gsql_bool, {gsql_date, gsql_date},
          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"boollt",
       "$less",
       {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"bytealt",
       "$less",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8lt",
       "$less",
       {{{gsql_bool, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8lt",
       "$less",
       {{{gsql_bool, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"text_lt",
       "$less",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_lt",
                       "$less",
                       {{{gsql_bool,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_lt",
       "$less",
       {{{gsql_bool, {gsql_date, gsql_date},
          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"boolle",
       "$less_or_equal",
       {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"byteale",
       "$less_or_equal",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8le",
       "$less_or_equal",
       {{{gsql_bool, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8le",
       "$less_or_equal",
       {{{gsql_bool, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"text_le",
       "$less_or_equal",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_le",
                       "$less_or_equal",
                       {{{gsql_bool,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_le",
       "$less_or_equal",
       {{{gsql_bool, {gsql_date, gsql_date},
          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"boolge",
       "$greater_or_equal",
       {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"byteage",
       "$greater_or_equal",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8ge",
       "$greater_or_equal",
       {{{gsql_bool, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8ge",
       "$greater_or_equal",
       {{{gsql_bool, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"text_ge",
       "$greater_or_equal",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_ge",
                       "$greater_or_equal",
                       {{{gsql_bool,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_ge",
       "$greater_or_equal",
       {{{gsql_bool, {gsql_date, gsql_date},
          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"boolgt",
       "$greater",
       {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"byteagt",
       "$greater",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float8gt",
       "$greater",
       {{{gsql_bool, {gsql_double, gsql_double}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"int8gt",
       "$greater",
       {{{gsql_bool, {gsql_int64, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"text_gt",
       "$greater",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_gt",
                       "$greater",
                       {{{gsql_bool,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_gt",
       "$greater",
       {{{gsql_bool, {gsql_date, gsql_date},
          /*context_ptr=*/nullptr}}}});

  // TODO : Investigate and/or add support for between, in, and
  // in_array.
  functions.push_back(
      {"textlike",
       "$like",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"bytealike",
       "$like",
       {{{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"like",
       "$like",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_bool, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
}

void AddLogicFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddMiscellaneousFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  const zetasql::Type* gsql_int64_arr = zetasql::types::Int64ArrayType();
  const zetasql::Type* gsql_string_arr = zetasql::types::StringArrayType();
  const zetasql::Type* gsql_bool_arr = zetasql::types::BoolArrayType();
  const zetasql::Type* gsql_double_arr = zetasql::types::DoubleArrayType();
  const zetasql::Type* gsql_bytes_arr = zetasql::types::BytesArrayType();
  const zetasql::Type* gsql_timestamp_arr =
      zetasql::types::TimestampArrayType();
  const zetasql::Type* gsql_date_arr = zetasql::types::DateArrayType();
  const zetasql::Type* gsql_pg_numeric_array =
      spangres::types::PgNumericArrayMapping()->mapped_type();

  functions.push_back(
      {"random", "rand", {{{gsql_double, {}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"array_cat",
                       "array_concat",
                       {{{gsql_int64_arr,
                          {gsql_int64_arr, gsql_int64_arr},
                          /*context_ptr=*/nullptr}},
                        {{gsql_string_arr,
                          {gsql_string_arr, gsql_string_arr},
                          /*context_ptr=*/nullptr}},
                        {{gsql_bool_arr,
                          {gsql_bool_arr, gsql_bool_arr},
                          /*context_ptr=*/nullptr}},
                        {{gsql_double_arr,
                          {gsql_double_arr, gsql_double_arr},
                          /*context_ptr=*/nullptr}},
                        {{gsql_bytes_arr,
                          {gsql_bytes_arr, gsql_bytes_arr},
                          /*context_ptr=*/nullptr}},
                        {{gsql_timestamp_arr,
                          {gsql_timestamp_arr, gsql_timestamp_arr},
                          /*context_ptr=*/nullptr}},
                        {{gsql_pg_numeric_array,
                          {gsql_pg_numeric_array, gsql_pg_numeric_array},
                          /*context_ptr=*/nullptr}},
                        {{gsql_date_arr,
                          {gsql_date_arr, gsql_date_arr},
                          /*context_ptr=*/nullptr}}}});
  // Should be <required>, <required>, <optional>, but we don't support
  // optional. Workaround is to put in both variants as separate signatures.
  // Note: byte array is supported by googlesql but intentionally ommitted
  // because the signatures don't match on the second argument.
  functions.push_back({"array_to_string",
                       "array_to_string",
                       {
                           {{gsql_string,
                             {gsql_string_arr, gsql_string, gsql_string},
                             /*context_ptr=*/nullptr}},
                           {{gsql_string,
                             {gsql_string_arr, gsql_string},
                             /*context_ptr=*/nullptr}},
                       }});
}

void AddNetFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddHllCountFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddKllQuantilesFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddHashingFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
  functions.push_back(
      {"sha256",
       "sha256",
       {{{gsql_bytes, {gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"sha512",
       "sha512",
       {{{gsql_bytes, {gsql_bytes}, /*context_ptr=*/nullptr}}}});
}

void AddEncryptionFunctions(std::vector<PostgresFunctionArguments>& functions) {
}

void AddGeographyFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddAnonFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddSequenceFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  functions.push_back(
      {"nextval",
       "get_next_sequence_value",
       {{{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}}}});
}

}  // namespace postgres_translator
