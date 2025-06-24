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

#include "third_party/spanner_pg/catalog/builtin_spanner_functions.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/absl_log.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"  // IWYU pragma: keep

namespace postgres_translator {
namespace spangres {

namespace {

const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_float = zetasql::types::FloatType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
const zetasql::Type* gsql_date = zetasql::types::DateType();

const zetasql::Type* gsql_int64_array = zetasql::types::Int64ArrayType();
const zetasql::Type* gsql_string_array = zetasql::types::StringArrayType();
const zetasql::Type* gsql_bool_array = zetasql::types::BoolArrayType();
const zetasql::Type* gsql_float_array = zetasql::types::FloatArrayType();
const zetasql::Type* gsql_double_array = zetasql::types::DoubleArrayType();
const zetasql::Type* gsql_bytes_array = zetasql::types::BytesArrayType();
const zetasql::Type* gsql_date_array = zetasql::types::DateArrayType();
const zetasql::Type* gsql_timestamp_array =
    zetasql::types::TimestampArrayType();

using FunctionNameWithSignature =
    std::pair<absl::string_view,
              const std::vector<PostgresFunctionSignatureArguments>>;

void AddNewSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions,
    std::vector<FunctionNameWithSignature>& func_name_with_signatures) {
  auto add_signatures_for_name =
      [&functions](
          absl::string_view function_name,
          const std::vector<PostgresFunctionSignatureArguments>& signatures) {
        auto function = std::find_if(
            functions.begin(), functions.end(),
            [function_name](const PostgresFunctionArguments& function) {
              return function.postgres_function_name() == function_name;
            });
        for (const auto& signature : signatures) {
          function->add_signature(signature);
        }
      };
  for (const auto& [function_name, signatures] : func_name_with_signatures) {
    add_signatures_for_name(function_name, signatures);
  }
}

zetasql::FunctionArgumentTypeOptions GetRequiredNamedArgumentOptions(
    absl::string_view name) {
  zetasql::FunctionArgumentTypeOptions options;
  options.set_cardinality(zetasql::FunctionArgumentType::REQUIRED);
  options.set_argument_name(name, zetasql::kPositionalOrNamed);
  return options;
}

zetasql::FunctionArgumentTypeOptions GetOptionalNamedArgumentOptions(
    absl::string_view name) {
  zetasql::FunctionArgumentTypeOptions options;
  options.set_cardinality(zetasql::FunctionArgumentType::OPTIONAL);
  options.set_argument_name(name, zetasql::kNamedOnly);
  return options;
}

zetasql::FunctionArgumentTypeOptions GetRequiredPositionalArgumentOptions(
    absl::string_view name) {
  zetasql::FunctionArgumentTypeOptions options;
  options.set_cardinality(zetasql::FunctionArgumentType::REQUIRED);
  options.set_argument_name(name, zetasql::kPositionalOnly);
  return options;
}

zetasql::FunctionArgumentTypeOptions GetRequiredNamedOnlyArgumentOptions(
    absl::string_view name) {
  zetasql::FunctionArgumentTypeOptions options;
  options.set_cardinality(zetasql::FunctionArgumentType::REQUIRED);
  options.set_argument_name(name, zetasql::kNamedOnly);
  return options;
}

}  // namespace

static void AddPgNumericSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();
  const zetasql::Type* gsql_pg_numeric_array =
      types::PgNumericArrayMapping()->mapped_type();
  std::vector<FunctionNameWithSignature> functions_with_new_signatures = {
      {"array_cat",
       {{{gsql_pg_numeric_array,
          {gsql_pg_numeric_array, gsql_pg_numeric_array},
          /*context_ptr=*/nullptr}}}},
      {"abs",
       {{{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}}},
      {"ceil",
       {{{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}}},
      {"ceiling",
       {{{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}}},
      {"floor",
       {{{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}}},
      {"mod",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}},
      {"trunc",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_int64},
          /*context_ptr=*/nullptr}}}}};

    functions_with_new_signatures.push_back(
        {"min",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_min",
         }}});
    functions_with_new_signatures.push_back(
        {"max",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_max",
         }}});

    functions_with_new_signatures.push_back({"count",
                                             {{{gsql_int64,
                                                {gsql_pg_numeric},
                                                /*context_ptr=*/nullptr}}}});

  functions_with_new_signatures.push_back({"count",
                                            {{{gsql_int64,
                                              {gsql_pg_numeric_array},
                                              /*context_ptr=*/nullptr}}}});

  AddNewSignaturesForExistingFunctions(functions,
                                       functions_with_new_signatures);
}

static void AddPgNumericNewFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();
  functions.push_back(
      {"numeric_add",
       "$add",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_sub",
       "$subtract",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_div",
       "$divide",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_mul",
       "$multiply",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_abs",
       "abs",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_mod",
       "mod",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_div_trunc",
       "div",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"div",
       "div",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_uminus",
       "$unary_minus",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});

  // Basic comparison functions
  functions.push_back(
      {"numeric_eq",
       "$equal",
       {{{gsql_bool,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_ne",
       "$not_equal",
       {{{gsql_bool,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_lt",
       "$less",
       {{{gsql_bool,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_le",
       "$less_or_equal",
       {{{gsql_bool,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_ge",
       "$greater_or_equal",
       {{{gsql_bool,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"numeric_gt",
       "$greater",
       {{{gsql_bool,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr}}}});

  constexpr zetasql::Function::Mode AGGREGATE =
      zetasql::Function::AGGREGATE;

  std::vector<PostgresFunctionSignatureArguments> sum_signatures = {
      {{gsql_pg_numeric, {gsql_int64}, /*context_ptr=*/nullptr}},
      {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}},
      {{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}};

  std::vector<PostgresFunctionSignatureArguments> avg_signatures = {
      {{gsql_pg_numeric, {gsql_int64}, /*context_ptr=*/nullptr}},
      {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}},
      {{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}};

    sum_signatures.push_back(
        {{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}});
    avg_signatures.push_back(
        {{gsql_double, {gsql_float}, /*context_ptr=*/nullptr}});

  // Remove existing function registration for sum.
  functions.erase(std::remove_if(functions.begin(), functions.end(),
                                 [](const PostgresFunctionArguments& args) {
                                   return args.postgres_function_name() ==
                                          "sum";
                                 }),
                  functions.end());

  // Register new function mapping for sum.
  functions.push_back({"sum", "pg.sum", sum_signatures, AGGREGATE});

  // Remove existing function registration for avg.
  functions.erase(std::remove_if(functions.begin(), functions.end(),
                                 [](const PostgresFunctionArguments& args) {
                                   return args.postgres_function_name() ==
                                          "avg";
                                 }),
                  functions.end());
  functions.push_back({"avg", "pg.avg", avg_signatures, AGGREGATE});
}

void AddPgNumericFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgNumericSignaturesForExistingFunctions(functions);
  AddPgNumericNewFunctions(functions);
}

static void AddPgJsonbSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_jsonb =
      types::PgJsonbMapping()->mapped_type();
  const zetasql::Type* gsql_pg_jsonb_arr =
      types::PgJsonbArrayMapping()->mapped_type();

    std::vector<FunctionNameWithSignature>
        existing_jsonb_functions_with_signature = {
            {"array_agg",
             {{{gsql_pg_jsonb_arr,
                {gsql_pg_jsonb},
                /*context_ptr=*/nullptr}}}}};
      existing_jsonb_functions_with_signature.push_back(
          {"count",
           {{{gsql_int64,
              {gsql_pg_jsonb},
              /*context_ptr=*/nullptr}}}});

    existing_jsonb_functions_with_signature.push_back({
      "count",
      {{{gsql_int64, {gsql_pg_jsonb_arr},
          /*context_ptr=*/nullptr}}}
    });

    AddNewSignaturesForExistingFunctions(
        functions, existing_jsonb_functions_with_signature);
}

static void AddPgJsonbNewFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_jsonb = types::PgJsonbMapping()->mapped_type();
  const zetasql::Type* gsql_date = zetasql::types::DateType();
  const zetasql::Type* gsql_int64_arr = zetasql::types::Int64ArrayType();
  const zetasql::Type* gsql_string_arr = zetasql::types::StringArrayType();
  const zetasql::Type* gsql_bool_arr = zetasql::types::BoolArrayType();
  const zetasql::Type* gsql_double_arr = zetasql::types::DoubleArrayType();
  const zetasql::Type* gsql_bytes_arr = zetasql::types::BytesArrayType();
  const zetasql::Type* gsql_timestamp_arr =
      zetasql::types::TimestampArrayType();
  const zetasql::Type* gsql_date_arr = zetasql::types::DateArrayType();
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();
  const zetasql::Type* gsql_pg_numeric_arr =
      types::PgNumericArrayMapping()->mapped_type();
  const zetasql::Type* gsql_pg_jsonb_arr =
      types::PgJsonbArrayMapping()->mapped_type();
  auto gsql_pg_oid = types::PgOidMapping()->mapped_type();
  auto gsql_pg_oid_arr = types::PgOidArrayMapping()->mapped_type();

  functions.push_back(
      {"to_jsonb",
       "pg.to_jsonb",
       {{{gsql_pg_jsonb, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_double}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_bool}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_bytes}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_timestamp}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_date}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_jsonb}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_oid}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_int64_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_string_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_bool_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_double_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_bytes_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_timestamp_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_date_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_numeric_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_jsonb_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_oid_arr}, /*context_ptr=*/nullptr}}}});

  functions.push_back({"jsonb_typeof",
                       "json_type",
                       {{{gsql_string,
                          {gsql_pg_jsonb},
                          /*context_ptr=*/nullptr}}}});

  // Register new function mapping for '->' operator on JSONB arrays.
  functions.push_back({"jsonb_array_element",
                       "$subscript",
                       {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_int64},
                          /*context_ptr=*/nullptr}}}});

  // Register new function mapping for '->' operator on JSONB objects.
  functions.push_back({"jsonb_object_field",
                       "$subscript",
                       {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_string},
                          /*context_ptr=*/nullptr}}}});

  // Register new function mapping for '->>' operator on JSONB arrays.
  functions.push_back({"jsonb_array_element_text",
                       "pg.jsonb_subscript_text",
                       {{{gsql_string,
                          {gsql_pg_jsonb, gsql_int64},
                          /*context_ptr=*/nullptr}}}});
  // Register new function mapping for '->>' operator on JSONB objects.
  functions.push_back({"jsonb_object_field_text",
                       "pg.jsonb_subscript_text",
                       {{{gsql_string,
                          {gsql_pg_jsonb, gsql_string},
                          /*context_ptr=*/nullptr}}}});

    // Register new function mapping '@>' operator on JSONB.
  functions.push_back({"jsonb_contains",
                       "pg.jsonb_contains",
                       {{{gsql_bool,
                          {gsql_pg_jsonb, gsql_pg_jsonb},
                          /*context_ptr=*/nullptr}}}});
  // Register new function mapping for '@<' operator on JSONB.
  functions.push_back({"jsonb_contained",
                       "pg.jsonb_contained",
                       {{{gsql_bool,
                          {gsql_pg_jsonb, gsql_pg_jsonb},
                          /*context_ptr=*/nullptr}}}});

  // Register new function mapping for '?' operator on JSONB.
  functions.push_back({"jsonb_exists",
                       "pg.jsonb_exists",
                       {{{gsql_bool,
                          {gsql_pg_jsonb, gsql_string},
                          /*context_ptr=*/nullptr}}}});
  // Register new function mapping for '?|' operator on JSONB.
  functions.push_back({"jsonb_exists_any",
                       "pg.jsonb_exists_any",
                       {{{gsql_bool,
                          {gsql_pg_jsonb, gsql_string_arr},
                          /*context_ptr=*/nullptr}}}});
  // Register new function mapping for '?&' operator on JSONB.
  functions.push_back({"jsonb_exists_all",
                       "pg.jsonb_exists_all",
                       {{{gsql_bool,
                          {gsql_pg_jsonb, gsql_string_arr},
                          /*context_ptr=*/nullptr}}}});
  // Register new function mapping for `-` operator on JSONB.
  functions.push_back({"jsonb_delete",
                        "pg.jsonb_delete",
                        {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_string},
                          /*context_ptr=*/nullptr}},
                        {{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_int64},
                          /*context_ptr=*/nullptr}}}});

  // Register new function mapping for `#-` operator on JSONB.
  functions.push_back({"jsonb_delete_path",
                        "pg.jsonb_delete_path",
                        {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_string_arr},
                          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"jsonb_set",
        "pg.jsonb_set",
        {{{gsql_pg_jsonb,
          {gsql_pg_jsonb, gsql_string_arr, gsql_pg_jsonb, gsql_bool},
          /*context_ptr=*/nullptr}}}});

  functions.push_back({"jsonb_set_lax",
                        "pg.jsonb_set_lax",
                        {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_string_arr, gsql_pg_jsonb,
                            gsql_bool, gsql_string},
                          /*context_ptr=*/nullptr}}}});

  functions.push_back({"jsonb_concat",
                        "pg.jsonb_concat",
                        {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_pg_jsonb},
                          /*context_ptr=*/nullptr}}}});

  functions.push_back(
      {"jsonb_insert",
        "pg.jsonb_insert",
        {{{gsql_pg_jsonb,
          {gsql_pg_jsonb, gsql_string_arr, gsql_pg_jsonb, gsql_bool},
          /*context_ptr=*/nullptr}}}});

  functions.push_back({"jsonb_strip_nulls",
                        "pg.jsonb_strip_nulls",
                        {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb},
                          /*context_ptr=*/nullptr}}}});

  zetasql::FunctionArgumentTypeOptions repeated(
      zetasql::FunctionArgumentType::REPEATED);
  functions.push_back(
      {"jsonb_build_object",
       "pg.jsonb_build_object",
       {{{gsql_pg_jsonb,
          {{gsql_string, zetasql::FunctionArgumentType::REPEATED},
           {zetasql::SignatureArgumentKind::ARG_TYPE_ARBITRARY,
            zetasql::FunctionArgumentType::REPEATED}},
          /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"",
         F_JSONB_BUILD_OBJECT_ANY},
        {{gsql_pg_jsonb, {}, /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"",
         F_JSONB_BUILD_OBJECT_}}});

  functions.push_back(
      {"jsonb_build_array",
        "pg.jsonb_build_array",
        {{{gsql_pg_jsonb,
          {{zetasql::SignatureArgumentKind::ARG_TYPE_ARBITRARY,
            zetasql::FunctionArgumentType::REPEATED}},
          /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"",
          F_JSONB_BUILD_ARRAY_ANY},
        {{gsql_pg_jsonb, {}, /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"",
          F_JSONB_BUILD_ARRAY_}}});

  functions.push_back(
      {"jsonb_query_array",
       "json_query_array",
       {{{gsql_pg_jsonb_arr, {gsql_pg_jsonb}, /*context_ptr=*/nullptr}}},
       /*mode=*/zetasql::Function::SCALAR,
       /*postgres_namespace=*/"spanner"});
}

void AddPgJsonbFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgJsonbSignaturesForExistingFunctions(functions);
  AddPgJsonbNewFunctions(functions);
}

static void AddPgOidSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  auto gsql_pg_oid = types::PgOidMapping()->mapped_type();
  auto gsql_pg_oid_arr = types::PgOidArrayMapping()->mapped_type();

  std::vector<FunctionNameWithSignature>
  functions_with_new_signatures = {
    // Scalar functions.
    {"array_cat",
      {{{gsql_pg_oid_arr, {gsql_pg_oid_arr, gsql_pg_oid_arr},
        /*context_ptr=*/nullptr}}}},
    // Aggregate functions.
    {"array_agg",
      {{{gsql_pg_oid_arr, {gsql_pg_oid}, /*context_ptr=*/nullptr}}}},
    {"count",
      {{{gsql_int64, {gsql_pg_oid}, /*context_ptr=*/nullptr}}}},
  };

    functions_with_new_signatures.push_back(
        {"min",
         {{
             {gsql_pg_oid, {gsql_pg_oid}, /*context_ptr=*/nullptr},
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.min",
         }}});
    functions_with_new_signatures.push_back(
        {"max",
         {{
             {gsql_pg_oid, {gsql_pg_oid}, /*context_ptr=*/nullptr},
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.max",
         }}});

  AddNewSignaturesForExistingFunctions(
      functions, functions_with_new_signatures);
}

static void AddPgOidSignaturesForNewFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_oid = types::PgOidMapping()->mapped_type();

  functions.push_back(
    {"oideq",
    "$equal",
    {{{gsql_bool, {gsql_oid, gsql_oid}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
    {"oidne",
    "$not_equal",
    {{{gsql_bool, {gsql_oid, gsql_oid}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
    {"oidlt",
    "$less",
    {{{gsql_bool, {gsql_oid, gsql_oid}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
    {"oidle",
    "$less_or_equal",
    {{{gsql_bool, {gsql_oid, gsql_oid}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
    {"oidgt",
    "$greater",
    {{{gsql_bool, {gsql_oid, gsql_oid}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
    {"oidge",
    "$greater_or_equal",
    {{{gsql_bool, {gsql_oid, gsql_oid}, /*context_ptr=*/nullptr}}}});
}

void AddPgOidFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgOidSignaturesForExistingFunctions(functions);
  AddPgOidSignaturesForNewFunctions(functions);
}

static void AddFloatSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_float = zetasql::types::FloatType();
  const zetasql::Type* gsql_float_arr = zetasql::types::FloatArrayType();
  const zetasql::Type* gsql_pg_jsonb = types::PgJsonbMapping()->mapped_type();

  std::vector<FunctionNameWithSignature>
      existing_float_functions_with_signature;
  existing_float_functions_with_signature.push_back(
      {"abs", {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"count", {{{gsql_int64, {gsql_float}, /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"count", {{{gsql_int64, {gsql_float_arr}, /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"array_agg",
       {{{gsql_float_arr, {gsql_float}, /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"array_cat",
       {{{gsql_float_arr,
          {gsql_float_arr, gsql_float_arr},
          /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"array_upper",
       {{{gsql_int64,
          {gsql_float_arr, gsql_int64},
          /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"array_length",
       {{{gsql_int64,
          {gsql_float_arr, gsql_int64},
          /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"euclidean_distance",
       {{{gsql_double,
          {gsql_float_arr, gsql_float_arr},
          /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"cosine_distance",
       {{{gsql_double,
          {gsql_float_arr, gsql_float_arr},
          /*context_ptr=*/nullptr}}}});

    existing_float_functions_with_signature.push_back(
        {"min", {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});
    existing_float_functions_with_signature.push_back(
        {"max", {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});

    existing_float_functions_with_signature.push_back(
        {"to_char",
         {{{gsql_string,
            {gsql_float, gsql_string},
            /*context_ptr=*/nullptr}}}});

      existing_float_functions_with_signature.push_back(
          {"to_jsonb",
           {{{gsql_pg_jsonb, {gsql_float}, /*context_ptr=*/nullptr}}}});

        existing_float_functions_with_signature.push_back(
            {"to_jsonb",
             {{{gsql_pg_jsonb, {gsql_float_arr}, /*context_ptr=*/nullptr}}}});

  AddNewSignaturesForExistingFunctions(functions,
                                       existing_float_functions_with_signature);
}

static void AddFloatNewFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_double = zetasql::types::DoubleType();
  const zetasql::Type* gsql_float = zetasql::types::FloatType();

  const zetasql::Type* gsql_double_array =
      zetasql::types::DoubleArrayType();
  const zetasql::Type* gsql_float_array = zetasql::types::FloatArrayType();
  const zetasql::Type* gsql_int64_array = zetasql::types::Int64ArrayType();

  functions.push_back(
      {"float4um",
       "$unary_minus",
       {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"float4abs",
       "abs",
       {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});

    functions.push_back(
        {"float4pl",
         "pg.float_add",
         {{{gsql_float, {gsql_float, gsql_float}, /*context_ptr=*/nullptr}}}});
    functions.push_back(
        {"float4mi",
         "pg.float_subtract",
         {{{gsql_float, {gsql_float, gsql_float}, /*context_ptr=*/nullptr}}}});
    functions.push_back(
        {"float4mul",
         "pg.float_multiply",
         {{{gsql_float, {gsql_float, gsql_float}, /*context_ptr=*/nullptr}}}});
    functions.push_back(
        {"float4div",
         "pg.float_divide",
         {{{gsql_float, {gsql_float, gsql_float}, /*context_ptr=*/nullptr}}}});

    functions.push_back({"float4eq",
                         "$equal",
                         {{{gsql_bool,
                            {gsql_float, gsql_float},
                            /*context_ptr=*/nullptr}}}});
    functions.push_back({"float4ne",
                         "$not_equal",
                         {{{gsql_bool,
                            {gsql_float, gsql_float},
                            /*context_ptr=*/nullptr}}}});
    functions.push_back({"float4lt",
                         "$less",
                         {{{gsql_bool,
                            {gsql_float, gsql_float},
                            /*context_ptr=*/nullptr}}}});
    functions.push_back({"float4le",
                         "$less_or_equal",
                         {{{gsql_bool,
                            {gsql_float, gsql_float},
                            /*context_ptr=*/nullptr}}}});
    functions.push_back({"float4gt",
                         "$greater",
                         {{{gsql_bool,
                            {gsql_float, gsql_float},
                            /*context_ptr=*/nullptr}}}});
    functions.push_back({"float4ge",
                         "$greater_or_equal",
                         {{{gsql_bool,
                            {gsql_float, gsql_float},
                            /*context_ptr=*/nullptr}}}});

  functions.push_back({"dot_product",
                       "dot_product",
                       {{{gsql_double,
                          {gsql_int64_array, gsql_int64_array},
                          /*context_ptr=*/nullptr}},
                        {{gsql_double,
                          {gsql_double_array, gsql_double_array},
                          /*context_ptr=*/nullptr}},
                        {{gsql_double,
                          {gsql_float_array, gsql_float_array},
                          /*context_ptr=*/nullptr}}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});
}

void AddFloatFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddFloatSignaturesForExistingFunctions(functions);
  AddFloatNewFunctions(functions);
}

static void AddFullTextSearchNewFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_tokenlist = zetasql::types::TokenListType();
  const zetasql::Type* gsql_tokenlist_array =
      zetasql::types::TokenListArrayType();
  const zetasql::Type* gsql_pg_jsonb = types::PgJsonbMapping()->mapped_type();

    functions.push_back({"token",
                         "token",
                         {{{gsql_tokenlist,
                            {gsql_string},
                            /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50020},
                          {{gsql_tokenlist,
                            {gsql_bytes},
                            /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50022}},
                         /*mode=*/zetasql::Function::SCALAR,
                         /*postgres_namespace=*/"spanner"});
  zetasql::FunctionArgumentTypeOptions value =
      GetRequiredNamedArgumentOptions("value");
  zetasql::FunctionArgumentTypeOptions language_tag =
      GetOptionalNamedArgumentOptions("language_tag");
  zetasql::FunctionArgumentTypeOptions content_type =
      GetOptionalNamedArgumentOptions("content_type");
  zetasql::FunctionArgumentTypeOptions token_category =
      GetOptionalNamedArgumentOptions("token_category");

  functions.push_back({"tokenize_fulltext",
                       "tokenize_fulltext",
                       {{{gsql_tokenlist,
                          {{gsql_string, value},
                           {gsql_string, language_tag},
                           {gsql_string, content_type},
                           {gsql_string, token_category}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50016},
                        {{gsql_tokenlist,
                          {{gsql_string_array, value},
                           {gsql_string, language_tag},
                           {gsql_string, content_type},
                           {gsql_string, token_category}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50017}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  zetasql::FunctionArgumentTypeOptions ngram_size_max =
      GetOptionalNamedArgumentOptions("ngram_size_max");
  zetasql::FunctionArgumentTypeOptions ngram_size_min =
      GetOptionalNamedArgumentOptions("ngram_size_min");
  zetasql::FunctionArgumentTypeOptions relative_search_types =
      GetOptionalNamedArgumentOptions("relative_search_types");
  zetasql::FunctionArgumentTypeOptions short_tokens_only_for_anchors =
      GetOptionalNamedArgumentOptions("short_tokens_only_for_anchors");
  zetasql::FunctionArgumentTypeOptions remove_diacritics =
      GetOptionalNamedArgumentOptions("remove_diacritics");

  functions.push_back({"tokenize_substring",
                       "tokenize_substring",
                       {{{gsql_tokenlist,
                          {{gsql_string, value},
                           {gsql_int64, ngram_size_max},
                           {gsql_int64, ngram_size_min},
                           {gsql_string, content_type},
                           {gsql_string_array, relative_search_types},
                           {gsql_bool, short_tokens_only_for_anchors},
                           {gsql_string, language_tag},
                           {gsql_bool, remove_diacritics}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50018},
                        {{gsql_tokenlist,
                          {{gsql_string_array, value},
                           {gsql_int64, ngram_size_max},
                           {gsql_int64, ngram_size_min},
                           {gsql_string, content_type},
                           {gsql_string_array, relative_search_types},
                           {gsql_bool, short_tokens_only_for_anchors},
                           {gsql_string, language_tag},
                           {gsql_bool, remove_diacritics}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50019}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  zetasql::FunctionArgumentTypeOptions comparison_type =
      GetOptionalNamedArgumentOptions("comparison_type");
  zetasql::FunctionArgumentTypeOptions algorithm =
      GetOptionalNamedArgumentOptions("algorithm");
  zetasql::FunctionArgumentTypeOptions min =
      GetOptionalNamedArgumentOptions("min");
  zetasql::FunctionArgumentTypeOptions max =
      GetOptionalNamedArgumentOptions("max");
  zetasql::FunctionArgumentTypeOptions granularity =
      GetOptionalNamedArgumentOptions("granularity");
  zetasql::FunctionArgumentTypeOptions tree_base =
      GetOptionalNamedArgumentOptions("tree_base");
  zetasql::FunctionArgumentTypeOptions ieee_precision =
      GetOptionalNamedArgumentOptions("ieee_precision");
  zetasql::FunctionSignatureOptions deprecated_options;

    functions.push_back({"tokenize_number",
                         "tokenize_number",
                         {{{gsql_tokenlist,
                            {{gsql_int64, value},
                             {gsql_string, comparison_type},
                             {gsql_string, algorithm},
                             {gsql_int64, min},
                             {gsql_int64, max},
                             {gsql_int64, granularity},
                             {gsql_int64, tree_base},
                             {gsql_int64, ieee_precision}},
                            /*context_id=*/0,
                            deprecated_options},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50026}},
                         /*mode=*/zetasql::Function::SCALAR,
                         /*postgres_namespace=*/"spanner"});

  functions.push_back({"tokenize_bool",
                       "tokenize_bool",
                       {{{gsql_tokenlist,
                          {{gsql_bool, value}}, /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50030}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back({"tokenize_ngrams",
                       "tokenize_ngrams",
                       {{{gsql_tokenlist,
                          {{gsql_string, value},
                           {gsql_int64, ngram_size_max},
                           {gsql_int64, ngram_size_min},
                           {gsql_bool, remove_diacritics}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50031},
                        {{gsql_tokenlist,
                          {{gsql_string_array, value},
                           {gsql_int64, ngram_size_max},
                           {gsql_int64, ngram_size_min},
                           {gsql_bool, remove_diacritics}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50057}},
                      /*mode=*/zetasql::Function::SCALAR,
                      /*postgres_namespace=*/"spanner"});

  zetasql::FunctionArgumentTypeOptions tokens =
      GetRequiredNamedArgumentOptions("tokens");

  functions.push_back({"tokenlist_concat",
                       "tokenlist_concat",
                       {{{gsql_tokenlist,
                          {{gsql_tokenlist_array, tokens}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50032}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  zetasql::FunctionArgumentTypeOptions query =
      GetRequiredNamedArgumentOptions("query");
  zetasql::FunctionArgumentTypeOptions relative_search_type =
      GetOptionalNamedArgumentOptions("relative_search_type");

  functions.push_back({"search_substring",
                       "search_substring",
                       {{{gsql_bool,
                          {{gsql_tokenlist, tokens},
                           {gsql_string, query},
                           {gsql_string, relative_search_type},
                           {gsql_string, language_tag}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50034}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  zetasql::FunctionArgumentTypeOptions ngrams_query =
      GetRequiredNamedArgumentOptions("ngrams_query");
  zetasql::FunctionArgumentTypeOptions min_ngrams =
      GetOptionalNamedArgumentOptions("min_ngrams");
  zetasql::FunctionArgumentTypeOptions min_ngrams_percent =
      GetOptionalNamedArgumentOptions("min_ngrams_percent");

  functions.push_back({"search_ngrams",
                       "search_ngrams",
                       {{{gsql_bool,
                          {{gsql_tokenlist, tokens},
                           {gsql_string, ngrams_query},
                           {gsql_int64, min_ngrams},
                           {gsql_double, min_ngrams_percent},
                           {gsql_string, language_tag}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50035}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back({"score_ngrams",
                       "score_ngrams",
                       {{{gsql_double,
                          {{gsql_tokenlist, tokens},
                           {gsql_string, ngrams_query},
                           {gsql_string, algorithm},
                           {gsql_string, language_tag}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/50036}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back({"soundex",
                       "soundex",
                         {{{gsql_string,
                            {gsql_string},
                            /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50037}},
                         /*mode=*/zetasql::Function::SCALAR,
                         /*postgres_namespace=*/"spanner"});

    zetasql::FunctionArgumentTypeOptions enhance_query =
        GetOptionalNamedArgumentOptions("enhance_query");
    zetasql::FunctionArgumentTypeOptions dialect =
        GetOptionalNamedArgumentOptions("dialect");
    zetasql::FunctionArgumentTypeOptions enhance_query_options =
        GetOptionalNamedArgumentOptions("enhance_query_options");
    functions.push_back({"search",
                         "search",
                         {{{gsql_bool,
                            {{gsql_tokenlist, tokens},
                             {gsql_string, query},
                             {gsql_bool, enhance_query},
                             {gsql_string, language_tag},
                             {gsql_string, dialect},
                             {gsql_pg_jsonb, enhance_query_options}},
                            /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50033}},
                         /*mode=*/zetasql::Function::SCALAR,
                         /*postgres_namespace=*/"spanner"});

    zetasql::FunctionArgumentTypeOptions options =
        GetOptionalNamedArgumentOptions("options");
    functions.push_back({"score",
                         "score",
                         {{{gsql_double,
                            {{gsql_tokenlist, tokens},
                             {gsql_string, query},
                             {gsql_bool, enhance_query},
                             {gsql_string, language_tag},
                             {gsql_string, dialect},
                             {gsql_pg_jsonb, enhance_query_options},
                             {gsql_pg_jsonb, options}},
                            /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50058}},
                         /*mode=*/zetasql::Function::SCALAR,
                         /*postgres_namespace=*/"spanner"});

    zetasql::FunctionArgumentTypeOptions max_snippet_width =
        GetOptionalNamedArgumentOptions("max_snippet_width");
    zetasql::FunctionArgumentTypeOptions max_snippets =
        GetOptionalNamedArgumentOptions("max_snippets");
    functions.push_back({"snippet",
                         "snippet",
                         {{{gsql_pg_jsonb,
                            {{gsql_string, value},
                             {gsql_string, query},
                             {gsql_bool, enhance_query},
                             {gsql_string, language_tag},
                             {gsql_int64, max_snippet_width},
                             {gsql_int64, max_snippets},
                             {gsql_string, content_type},
                             {gsql_pg_jsonb, enhance_query_options}},
                            /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50059}},
                         /*mode=*/zetasql::Function::SCALAR,
                         /*postgres_namespace=*/"spanner"});
};

void AddFullTextSearchFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  AddFullTextSearchNewFunctions(functions);
};

void AddPgArrayFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_jsonb_arr =
      types::PgJsonbArrayMapping()->mapped_type();
  const zetasql::Type* gsql_pg_numeric_arr =
      types::PgNumericArrayMapping()->mapped_type();

  PostgresFunctionArguments array_upper_function(
      {"array_upper",
       "pg.array_upper",
       {{{gsql_int64,
          {gsql_int64_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_string_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_bool_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_double_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_bytes_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_date_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_timestamp_array, gsql_int64},
          /*context_ptr=*/nullptr}}}});

    array_upper_function.add_signature(
        {{gsql_int64,
          {gsql_pg_numeric_arr, gsql_int64},
          /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.array_upper"});

    array_upper_function.add_signature(
        {{gsql_int64,
          {gsql_pg_jsonb_arr, gsql_int64},
          /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.array_upper"});
  functions.push_back(array_upper_function);

  PostgresFunctionArguments array_length_function(
      {"array_length",
        "pg.array_length",
        {{{gsql_int64,
          {gsql_int64_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_string_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_bool_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_double_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_bytes_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_date_array, gsql_int64},
          /*context_ptr=*/nullptr}},
        {{gsql_int64,
          {gsql_timestamp_array, gsql_int64},
          /*context_ptr=*/nullptr}}}});

    array_length_function.add_signature(
        {{gsql_int64,
          {gsql_pg_numeric_arr, gsql_int64},
          /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"pg.array_length"});

    array_length_function.add_signature(
        {{gsql_int64,
          {gsql_pg_jsonb_arr, gsql_int64},
          /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"pg.array_length"});
  functions.push_back(array_length_function);

    PostgresFunctionArguments arraycontained_function(
        {"arraycontained",
        "pg.array_contained",
        {{{gsql_bool,
            {gsql_int64_array, gsql_int64_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_string_array, gsql_string_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_bool_array, gsql_bool_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_bytes_array, gsql_bytes_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_date_array, gsql_date_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_timestamp_array, gsql_timestamp_array},
            /*context_ptr=*/nullptr}}}});
      arraycontained_function.add_signature(
          {{gsql_bool,
            {gsql_pg_numeric_arr, gsql_pg_numeric_arr},
            /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"pg.array_contained"});
    // Intentionally omitted jsonb because spanner does not support json
    // equality.
    functions.push_back(arraycontained_function);

    PostgresFunctionArguments arraycontains_function(
        {"arraycontains",
        "pg.array_contains",
        {{{gsql_bool,
            {gsql_int64_array, gsql_int64_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_string_array, gsql_string_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_bool_array, gsql_bool_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_bytes_array, gsql_bytes_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_date_array, gsql_date_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_timestamp_array, gsql_timestamp_array},
            /*context_ptr=*/nullptr}}}});
      arraycontains_function.add_signature(
          {{gsql_bool,
            {gsql_pg_numeric_arr, gsql_pg_numeric_arr},
            /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"pg.array_contains"});
    // Intentionally omitted jsonb because spanner does not support json
    // equality.
    functions.push_back(arraycontains_function);

    PostgresFunctionArguments arrayoverlap_function(
        {"arrayoverlap",
        "pg.array_overlap",
        {{{gsql_bool,
            {gsql_int64_array, gsql_int64_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_string_array, gsql_string_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_bool_array, gsql_bool_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_bytes_array, gsql_bytes_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_date_array, gsql_date_array},
            /*context_ptr=*/nullptr}},
          {{gsql_bool,
            {gsql_timestamp_array, gsql_timestamp_array},
            /*context_ptr=*/nullptr}}}});
      arrayoverlap_function.add_signature(
          {{gsql_bool,
            {gsql_pg_numeric_arr, gsql_pg_numeric_arr},
            /*context_ptr=*/nullptr},
          /*has_mapped_function=*/true,
          /*explicit_mapped_function_name=*/"pg.array_overlap"});
    // Intentionally omitted jsonb because spanner does not support json
    // equality.
    functions.push_back(arrayoverlap_function);
}

void AddPgComparisonFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  functions.push_back(
      {"textregexne",
       "pg.textregexne",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
}

void AddPgDatetimeFunctions(std::vector<PostgresFunctionArguments>& functions) {
  functions.push_back(
      {"date_mi",
       "pg.date_mi",
       {{{gsql_int64, {gsql_date, gsql_date}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_mii",
       "pg.date_mii",
       {{{gsql_date, {gsql_date, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"date_pli",
       "pg.date_pli",
       {{{gsql_date, {gsql_date, gsql_int64}, /*context_ptr=*/nullptr}}}});
}

void AddPgFormattingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  functions.push_back(
      {"to_date",
       "pg.to_date",
       {{{gsql_date, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  PostgresFunctionArguments to_char_function(
      {"to_char",
       "pg.to_char",
       {{{gsql_string, {gsql_int64, gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_timestamp, gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_double, gsql_string}, /*context_ptr=*/nullptr}}}});

    const zetasql::Type* gsql_pg_numeric =
        types::PgNumericMapping()->mapped_type();
    functions.push_back({"to_number",
                         "pg.to_number",
                         {{{gsql_pg_numeric,
                            {gsql_string, gsql_string},
                            /*context_ptr=*/nullptr}}}});
    to_char_function.add_signature(
        {{gsql_string, {gsql_pg_numeric, gsql_string}, /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.to_char"});
  functions.push_back(to_char_function);

  // We need to find whether to_timestamp has already been registered and update
  // its signature if so. If not, we add a new function definition
  auto to_timestamp_function =
      absl::c_find_if(functions, [](const PostgresFunctionArguments& args) {
        return args.postgres_function_name() == "to_timestamp";
      });
  if (to_timestamp_function == functions.end()) {
    // Adds new function definition for to_timestamp
    functions.push_back({"to_timestamp",
                         "pg.to_timestamp",
                         {{{gsql_timestamp,
                            {gsql_string, gsql_string},
                            /*context_ptr=*/nullptr}}}});
  } else {
    // Adds to_timestamp signature to existing function definition
    to_timestamp_function->add_signature(
        {{gsql_timestamp, {gsql_string, gsql_string}, /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.to_timestamp"});
  }
}

void AddPgStringFunctions(std::vector<PostgresFunctionArguments>& functions) {
  functions.push_back(
      {"quote_ident",
       "pg.quote_ident",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"regexp_match",
                       "pg.regexp_match",
                       {{{gsql_string_array,
                          {gsql_string, gsql_string},
                          /*context_ptr=*/nullptr}},
                        {{gsql_string_array,
                          {gsql_string, gsql_string, gsql_string},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"regexp_split_to_array",
                       "pg.regexp_split_to_array",
                       {{{gsql_string_array,
                          {gsql_string, gsql_string},
                          /*context_ptr=*/nullptr}},
                        {{gsql_string_array,
                          {gsql_string, gsql_string, gsql_string},
                          /*context_ptr=*/nullptr}}}});

  // We need to find whether substring has already been registered and update
  // its signature if so. If not, we add a new function definition
  auto substring_function =
      absl::c_find_if(functions, [](const PostgresFunctionArguments& args) {
        return args.postgres_function_name() == "substring";
      });
  if (substring_function == functions.end()) {
    // Adds new function definition for substring
    functions.push_back({"substring",
                         "pg.substring",
                         {{{gsql_string,
                            {gsql_string, gsql_string},
                            /*context_ptr=*/nullptr}}}});
  } else {
    // Adds substring signature to existing function definition
    substring_function->add_signature(
        {{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr},
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.substring"});
  }
}

void AddSpannerFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();

  functions.push_back({"pending_commit_timestamp",
                       "pending_commit_timestamp",
                       {{{gsql_timestamp, {}, /*context_ptr=*/nullptr}}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back({"bit_reverse",
                       "bit_reverse",
                       {{{gsql_int64,
                          {gsql_int64, gsql_bool},
                          /*context_ptr=*/nullptr}}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back({"farm_fingerprint",
                       "farm_fingerprint",
                       {{{gsql_int64, {gsql_bytes}, /*context_ptr=*/nullptr}},
                        {{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back({"get_internal_sequence_state",
                       "get_internal_sequence_state",
                       {{{gsql_int64,
                          {gsql_string},
                          /*context_ptr=*/nullptr}}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

    functions.push_back(
        {"get_table_column_identity_state",
         "get_table_column_identity_state",
         {{{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}}},
         /*mode=*/zetasql::Function::SCALAR,
         /*postgres_namespace=*/"spanner"});

  functions.push_back({"generate_uuid",
                       "generate_uuid",
                       {{{gsql_string, {}, /*context_ptr=*/nullptr}}},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

  functions.push_back(
      {"timestamp_from_unix_micros",
       "timestamp_from_unix_micros",
       {{{gsql_timestamp, {gsql_int64}, /*context_ptr=*/nullptr}},
       {{gsql_timestamp, {gsql_timestamp}, /*context_ptr=*/nullptr}}},
       /*mode=*/zetasql::Function::SCALAR,
       /*postgres_namespace=*/"spanner"});

  functions.push_back(
      {"timestamp_from_unix_millis",
       "timestamp_from_unix_millis",
       {{{gsql_timestamp, {gsql_int64}, /*context_ptr=*/nullptr}},
       {{gsql_timestamp, {gsql_timestamp}, /*context_ptr=*/nullptr}}},
       /*mode=*/zetasql::Function::SCALAR,
       /*postgres_namespace=*/"spanner"});

  functions.push_back({"timestamptz_add",
      "pg.timestamptz_add",
      {{{gsql_timestamp,
         {gsql_timestamp, gsql_string},
         /*context_ptr=*/nullptr}}},
      /*mode=*/zetasql::Function::SCALAR,
      /*postgres_namespace=*/"spanner"});
  functions.push_back({"timestamptz_subtract",
      "pg.timestamptz_subtract",
      {{{gsql_timestamp,
         {gsql_timestamp, gsql_string},
         /*context_ptr=*/nullptr}}},
      /*mode=*/zetasql::Function::SCALAR,
      /*postgres_namespace=*/"spanner"});
  functions.push_back({"date_bin",
      "pg.date_bin",
      {{{gsql_timestamp,
         {gsql_string, gsql_timestamp, gsql_timestamp},
         /*context_ptr=*/nullptr}}},
      /*mode=*/zetasql::Function::SCALAR,
      /*postgres_namespace=*/"spanner"});
  functions.push_back({"date_trunc",
      "pg.date_trunc",
      {{{gsql_timestamp,
         {gsql_string, gsql_timestamp},
         /*context_ptr=*/nullptr}},
       {{gsql_timestamp,
         {gsql_string, gsql_timestamp, gsql_string},
         /*context_ptr=*/nullptr}}}});

    functions.push_back({"extract",
        "pg.extract",
        {{{gsql_pg_numeric,
           {gsql_string, gsql_timestamp},
           /*context_ptr=*/nullptr}},
         {{gsql_pg_numeric,
           {gsql_string, gsql_date},
           /*context_ptr=*/nullptr}}}});
  functions.push_back({"euclidean_distance",
      "euclidean_distance",
      {{{gsql_double,
         {gsql_double_array, gsql_double_array},
         /*context_ptr=*/nullptr}}},
      /*mode=*/zetasql::Function::SCALAR,
      /*postgres_namespace=*/"spanner"});
  functions.push_back({"cosine_distance",
      "cosine_distance",
      {{{gsql_double,
         {gsql_double_array, gsql_double_array},
         /*context_ptr=*/nullptr}}},
      /*mode=*/zetasql::Function::SCALAR,
      /*postgres_namespace=*/"spanner"});

    const zetasql::Type* gsql_pg_jsonb =
        types::PgJsonbMapping()->mapped_type();
    zetasql::FunctionArgumentTypeOptions model_endpoint =
        GetRequiredNamedArgumentOptions("model_endpoint");
    zetasql::FunctionArgumentTypeOptions args =
        GetRequiredNamedArgumentOptions("args");
    functions.push_back({"ml_predict_row",
                          "ml_predict_row",
                          {
                              {{gsql_pg_jsonb,
                                {{gsql_string, model_endpoint},
                                 {gsql_pg_jsonb, args}},
                                /*context_ptr=*/nullptr}},
                              {{gsql_pg_jsonb,
                                {{gsql_pg_jsonb, model_endpoint},
                                 {gsql_pg_jsonb, args}},
                                /*context_ptr=*/nullptr}},
                          },
                          /*mode=*/zetasql::Function::SCALAR,
                          /*postgres_namespace=*/"spanner"});

      functions.push_back({"int64_array",
                           "int64_array",
                           {{{gsql_int64_array,
                              {gsql_pg_jsonb},
                              /*context_ptr=*/nullptr}}},
                           /*mode=*/zetasql::Function::SCALAR,
                           /*postgres_namespace=*/"spanner"});
      functions.push_back({"float64_array",
                           "float64_array",
                           {{{gsql_double_array,
                              {gsql_pg_jsonb},
                              /*context_ptr=*/nullptr}}},
                           /*mode=*/zetasql::Function::SCALAR,
                           /*postgres_namespace=*/"spanner"});
      functions.push_back({"float32_array",
                           "float32_array",
                           {{{gsql_float_array,
                              {gsql_pg_jsonb},
                              /*context_ptr=*/nullptr}}},
                           /*mode=*/zetasql::Function::SCALAR,
                           /*postgres_namespace=*/"spanner"});
      functions.push_back({"bool_array",
                           "bool_array",
                           {{{gsql_bool_array,
                              {gsql_pg_jsonb},
                              /*context_ptr=*/nullptr}}},
                           /*mode=*/zetasql::Function::SCALAR,
                           /*postgres_namespace=*/"spanner"});
      functions.push_back({"string_array",
                           "string_array",
                           {{{gsql_string_array,
                              {gsql_pg_jsonb},
                              /*context_ptr=*/nullptr}}},
                           /*mode=*/zetasql::Function::SCALAR,
                           /*postgres_namespace=*/"spanner"});
}

void AddPgLeastGreatestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions) {
  auto res = functions.emplace(PostgresExprIdentifier::MinMaxExpr(IS_GREATEST),
                               "pg.greatest");
  ABSL_DCHECK(res.second) << "Duplicate key found for IS_GREATEST";
  res = functions.emplace(PostgresExprIdentifier::MinMaxExpr(IS_LEAST),
                          "pg.least");
  ABSL_DCHECK(res.second) << "Duplicate key found for IS_LEAST";
}

static void RemapMinFunction(
    std::vector<PostgresFunctionArguments>& functions) {
  auto min_function =
      absl::c_find_if(functions, [](const PostgresFunctionArguments& args) {
        return args.postgres_function_name() == "min";
      });

  if (min_function == functions.end()) {
    return;
  }

  // Create function signatures using existing signatures of `min_function`.
  std::vector<PostgresFunctionSignatureArguments> function_signatures;
  function_signatures.reserve(min_function->signature_arguments().size());

  for (const PostgresFunctionSignatureArguments& existing_signature :
       min_function->signature_arguments()) {
    if (existing_signature.signature().arguments().size() != 1) {
      ABSL_LOG(ERROR)
          << "Encountered min function signature with multiple arguments: "
          << existing_signature.signature().DebugString();
      continue;
    }
    const zetasql::Type* argumentType =
        existing_signature.signature().argument(0).type();

    // For float and double use PG.MIN function instead of default MIN to follow
    // Postgres' order semantics.
    if (argumentType->IsDouble()) {
      function_signatures.push_back(
          {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr},
           /*has_mapped_function=*/true,
           /*explicit_mapped_function_name=*/"pg.min"});
    } else if (argumentType->IsFloat()
              ) {
      function_signatures.push_back(
          {{gsql_float, {gsql_float}, /*context_ptr=*/nullptr},
           /*has_mapped_function=*/true,
           /*explicit_mapped_function_name=*/"pg.min"});
    } else {
      function_signatures.push_back(existing_signature);
    }
  }

  // Remove existing function registration for min.
  functions.erase(min_function);

  // Add new registration.
  functions.push_back(
      {"min", "min", function_signatures, zetasql::Function::AGGREGATE});
}

static void AddIntervalSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_interval = zetasql::types::IntervalType();
  const zetasql::Type* gsql_interval_array =
      zetasql::types::IntervalArrayType();
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();
  const zetasql::Type* gsql_pg_jsonb = types::PgJsonbMapping()->mapped_type();

  // Signatures for existing functions.
  std::vector<FunctionNameWithSignature>
      existing_functions_with_new_signatures = {
          {"to_char",
           {
               {{gsql_string,
                 {gsql_interval, gsql_string},
                 /*context_ptr=*/nullptr}},
           }},
          {"to_jsonb",
           {
               {{gsql_pg_jsonb,
                 {gsql_interval},
                 /*context_ptr=*/nullptr}},
               {{gsql_pg_jsonb,
                 {gsql_interval_array},
                 /*context_ptr=*/nullptr}},
           }},
          {"min",
           {{
               {gsql_interval, {gsql_interval}, /*context_ptr=*/nullptr},
               /*has_mapped_function=*/true,
               /*explicit_mapped_function_name=*/"min",
           }}},
          {"max",
           {{
               {gsql_interval, {gsql_interval}, /*context_ptr=*/nullptr},
               /*has_mapped_function=*/true,
               /*explicit_mapped_function_name=*/"max",
           }}},
          {"sum",
           {{
               {gsql_interval, {gsql_interval}, /*context_ptr=*/nullptr},
               /*has_mapped_function=*/true,
               /*explicit_mapped_function_name=*/"sum",
           }}},
          {"avg",
           {{
               {gsql_interval, {gsql_interval}, /*context_ptr=*/nullptr},
               /*has_mapped_function=*/true,
               /*explicit_mapped_function_name=*/"avg",
           }}},
          {"count",
           {{
               {gsql_int64, {gsql_interval}, /*context_ptr=*/nullptr},
           }}},
          {"array_agg",
           {{
               {gsql_interval_array, {gsql_interval}, /*context_ptr=*/nullptr},
           }}},
          {"array_cat",
           {{
               {gsql_interval_array,
                {gsql_interval_array, gsql_interval_array},
                /*context_ptr=*/nullptr},
           }}},
          {"array_length",
           {{
               {gsql_int64,
                {gsql_interval_array, gsql_int64},
                /*context_ptr=*/nullptr},
           }}},
          {"arraycontains",
           {{
               {gsql_bool,
                {gsql_interval_array, gsql_interval_array},
                /*context_ptr=*/nullptr},
           }}},
          {"arraycontained",
           {{
               {gsql_bool,
                {gsql_interval_array, gsql_interval_array},
                /*context_ptr=*/nullptr},
           }}},
          {"arrayoverlap",
           {{
               {gsql_bool,
                {gsql_interval_array, gsql_interval_array},
                /*context_ptr=*/nullptr},
           }}},
          {"array_upper",
           {{
               {gsql_int64,
                {gsql_interval_array, gsql_int64},
                /*context_ptr=*/nullptr},
           }}},
      };

    existing_functions_with_new_signatures.push_back(
        {"extract",
         {{
             {gsql_pg_numeric,
              {gsql_string, gsql_interval},
              /*context_ptr=*/nullptr},
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.extract_interval",
         }}});

  AddNewSignaturesForExistingFunctions(functions,
                                       existing_functions_with_new_signatures);
}

void AddIntervalFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_interval = zetasql::types::IntervalType();
  functions.push_back({"interval_eq",
                       "$equal",
                       {{{gsql_bool,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_ne",
                       "$not_equal",
                       {{{gsql_bool,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_lt",
                       "$less",
                       {{{gsql_bool,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_le",
                       "$less_or_equal",
                       {{{gsql_bool,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_gt",
                       "$greater",
                       {{{gsql_bool,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_ge",
                       "$greater_or_equal",
                       {{{gsql_bool,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_um",
                       "pg.interval_unary_minus",
                       {{{gsql_interval,
                          {gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_pl",
                       "pg.interval_add",
                       {{{gsql_interval,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_mi",
                       "pg.interval_subtract",
                       {{{gsql_interval,
                          {gsql_interval, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_mul",
                       "pg.interval_multiply",
                       {{{gsql_interval,
                          {gsql_interval, gsql_double},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"interval_div",
                       "pg.interval_divide",
                       {{{gsql_interval,
                          {gsql_interval, gsql_double},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"justify_hours",
                       "pg.justify_hours",
                       {{{gsql_interval,
                          {gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"justify_days",
                       "pg.justify_days",
                       {{{gsql_interval,
                          {gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"justify_interval",
                       "pg.justify_interval",
                       {{{gsql_interval,
                          {gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_pl_interval",
                       "pg.timestamptz_add",
                       {{{gsql_timestamp,
                          {gsql_timestamp, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"timestamptz_mi_interval",
                       "pg.timestamptz_subtract",
                       {{{gsql_timestamp,
                          {gsql_timestamp, gsql_interval},
                          /*context_ptr=*/nullptr}}}});
  zetasql::FunctionArgumentTypeOptions years =
      GetOptionalNamedArgumentOptions("years");
  zetasql::FunctionArgumentTypeOptions months =
      GetOptionalNamedArgumentOptions("months");
  zetasql::FunctionArgumentTypeOptions weeks =
      GetOptionalNamedArgumentOptions("weeks");
  zetasql::FunctionArgumentTypeOptions days =
      GetOptionalNamedArgumentOptions("days");
  zetasql::FunctionArgumentTypeOptions hours =
      GetOptionalNamedArgumentOptions("hours");
  zetasql::FunctionArgumentTypeOptions mins =
      GetOptionalNamedArgumentOptions("mins");
  zetasql::FunctionArgumentTypeOptions secs =
      GetOptionalNamedArgumentOptions("secs");
  functions.push_back({"make_interval",
                       "pg.make_interval",
                       {{{gsql_interval,
                          {{gsql_int64, years},
                           {gsql_int64, months},
                           {gsql_int64, weeks},
                           {gsql_int64, days},
                           {gsql_int64, hours},
                           {gsql_int64, mins},
                           {gsql_double, secs}},
                          /*context_ptr=*/nullptr},
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"",
                          /*postgres_proc_oid=*/F_MAKE_INTERVAL}}});
  functions.push_back({"timestamptz_mi",
                       "pg.timestamptz_subtract_timestamptz",
                       {{{gsql_interval,
                          {gsql_timestamp, gsql_timestamp},
                          /*context_ptr=*/nullptr}}}});

  AddIntervalSignaturesForExistingFunctions(functions);
}

void RemapFunctionsForSpanner(
    std::vector<PostgresFunctionArguments>& functions) {
  RemapMinFunction(functions);
}

}  // namespace spangres
}  // namespace postgres_translator
