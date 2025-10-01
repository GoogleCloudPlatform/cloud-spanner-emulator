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

}  // namespace

static void AddPgNumericSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();
  const zetasql::Type* gsql_pg_numeric_array =
      types::PgNumericArrayMapping()->mapped_type();
  std::vector<FunctionNameWithSignature> functions_with_new_signatures = {
      {"min",
       {{{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}}},
      {"max",
       {{{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}}},
  };

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

  // TODO: b/446754242 - Automate registration of jsonb_build_object
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

  // TODO: b/446754242 - Automate registration of jsonb_build_array
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
}

void AddPgJsonbFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgJsonbSignaturesForExistingFunctions(functions);
  AddPgJsonbNewFunctions(functions);
}

static void AddPgOidSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  auto gsql_pg_oid = types::PgOidMapping()->mapped_type();
  auto gsql_pg_oid_arr = types::PgOidArrayMapping()->mapped_type();

  std::vector<FunctionNameWithSignature> functions_with_new_signatures = {
      // Aggregate functions.
      {"array_agg",
       {{{gsql_pg_oid_arr, {gsql_pg_oid}, /*context_ptr=*/nullptr}}}},
      {"count", {{{gsql_int64, {gsql_pg_oid}, /*context_ptr=*/nullptr}}}},
      {"min", {{{gsql_pg_oid, {gsql_pg_oid}, /*context_ptr*/ nullptr}}}},
      {"max", {{{gsql_pg_oid, {gsql_pg_oid}, /*context_ptr*/ nullptr}}}},
  };

  AddNewSignaturesForExistingFunctions(
      functions, functions_with_new_signatures);
}

void AddPgOidFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgOidSignaturesForExistingFunctions(functions);
}

static void AddFloatSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_float = zetasql::types::FloatType();
  const zetasql::Type* gsql_float_arr = zetasql::types::FloatArrayType();

  std::vector<FunctionNameWithSignature>
      existing_float_functions_with_signature;
  existing_float_functions_with_signature.push_back(
      {"count", {{{gsql_int64, {gsql_float}, /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"count", {{{gsql_int64, {gsql_float_arr}, /*context_ptr=*/nullptr}}}});
  existing_float_functions_with_signature.push_back(
      {"array_agg",
       {{{gsql_float_arr, {gsql_float}, /*context_ptr=*/nullptr}}}});

    existing_float_functions_with_signature.push_back(
        {"min", {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});
    existing_float_functions_with_signature.push_back(
        {"max", {{{gsql_float, {gsql_float}, /*context_ptr=*/nullptr}}}});

  AddNewSignaturesForExistingFunctions(functions,
                                       existing_float_functions_with_signature);
}

void AddFloatFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddFloatSignaturesForExistingFunctions(functions);
}

void AddFullTextSearchFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
};

void AddPgFormattingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  // TODO: b/446759075 - Automate registration of to_timestamp function
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
  // TODO: b/446765173 - Automate registration of substring function
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
  // TODO: b/446758885 - Automate registration of pending_commit_timestamp
  functions.push_back({"pending_commit_timestamp",
                       "pending_commit_timestamp",
                       {{
                           {gsql_timestamp, {}, /*context_ptr=*/nullptr},
                           /*has_mapped_function=*/true,
                           /*explicit_mapped_function_name=*/"",
                           /*postgres_proc_oid=*/50005,
                       }},
                       /*mode=*/zetasql::Function::SCALAR,
                       /*postgres_namespace=*/"spanner"});

    // TODO: b/446758553 - Automate registration of date
    functions.push_back({"date",
                         "date",
                         {{
                             {gsql_date,
                              {gsql_timestamp, gsql_string},
                              /*context_ptr=*/nullptr},
                             /*has_mapped_function=*/true,
                             /*explicit_mapped_function_name=*/"",
                             /*postgres_proc_oid=*/50067,
                         }},
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

  // Signatures for existing functions.
  std::vector<FunctionNameWithSignature>
      existing_functions_with_new_signatures = {
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
  AddIntervalSignaturesForExistingFunctions(functions);
}

void RemapFunctionsForSpanner(
    std::vector<PostgresFunctionArguments>& functions) {
  RemapMinFunction(functions);
}

}  // namespace spangres
}  // namespace postgres_translator
