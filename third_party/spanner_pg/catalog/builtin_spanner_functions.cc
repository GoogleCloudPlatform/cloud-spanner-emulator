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

#include "zetasql/public/function.h"
#include "absl/flags/flag.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/type.h"

namespace postgres_translator {
namespace spangres {

namespace {
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
const zetasql::Type* gsql_date = zetasql::types::DateType();

const zetasql::Type* gsql_int64_array = zetasql::types::Int64ArrayType();
const zetasql::Type* gsql_string_array = zetasql::types::StringArrayType();
const zetasql::Type* gsql_bool_array = zetasql::types::BoolArrayType();
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

void AddPgArrayFunctions(std::vector<PostgresFunctionArguments>& functions) {
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

  functions.push_back(array_upper_function);
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
         /*has_postgres_proc_oid=*/true,
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
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.substring"});
  }
}

void AddSpannerFunctions(std::vector<PostgresFunctionArguments>& functions) {
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

  functions.push_back({"get_internal_sequence_state",
                       "get_internal_sequence_state",
                       {{{gsql_int64,
                          {gsql_string},
                          /*context_ptr=*/nullptr}}},
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
      ABSL_LOG(FATAL)
          << "Encountered min function signature with multiple arguments: "
          << existing_signature.signature().DebugString();
      continue;
    }
    const zetasql::Type* argumentType =
        existing_signature.signature().argument(0).type();

    if (!argumentType->IsDouble()) {
      function_signatures.push_back(existing_signature);
    } else {
      // For double use PG.MIN function instead of default MIN to follow
      // Postgres' order semantics.
      function_signatures.push_back(
          {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr},
           /*has_postgres_proc_oid=*/true,
           /*has_mapped_function=*/true,
           /*explicit_mapped_function_name=*/"pg.min"});
    }
  }

  // Remove existing function registration for min.
  functions.erase(min_function);

  // Add new registration.
  functions.push_back(
      {"min", "min", function_signatures, zetasql::Function::AGGREGATE});
}

void RemapFunctionsForSpanner(
    std::vector<PostgresFunctionArguments>& functions) {
  RemapMinFunction(functions);
}
}  // namespace spangres
}  // namespace postgres_translator
