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

#include "zetasql/public/function.h"
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
#include "third_party/spanner_pg/postgres_includes/all.h"

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
          /*context_ptr=*/nullptr}}}}};

    functions_with_new_signatures.push_back(
        {"min",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_min",
         }}});
    functions_with_new_signatures.push_back(
        {"max",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_max",
         }}});
    functions_with_new_signatures.push_back(
        {"abs",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_abs",
         }}});
    functions_with_new_signatures.push_back(
        {"ceil",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_ceil",
         }}});
    functions_with_new_signatures.push_back(
        {"ceiling",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_ceiling",
         }}});
    functions_with_new_signatures.push_back(
        {"floor",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_floor",
         }}});
    functions_with_new_signatures.push_back(
        {"mod",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric, gsql_pg_numeric},
             /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_mod",
         }}});
    functions_with_new_signatures.push_back(
        {"trunc",
         {{
             {gsql_pg_numeric, {gsql_pg_numeric, gsql_int64},
             /*context_ptr=*/nullptr},
             /*has_postgres_proc_oid=*/true,
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.numeric_trunc",
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
  // Basic arithmetic functions
  std::string emulator_add_fn_name = "pg.numeric_add";
  std::string emulator_subtract_fn_name = "pg.numeric_subtract";
  std::string emulator_divide_fn_name = "pg.numeric_divide";
  std::string emulator_multiply_fn_name = "pg.numeric_multiply";
  std::string emulator_abs_fn_name = "pg.numeric_abs";
  std::string emulator_mod_fn_name = "pg.numeric_mod";
  std::string emulator_div_trunc_fn_name = "pg.numeric_div_trunc";
  std::string emulator_uminus_fn_name = "pg.numeric_uminus";
  functions.push_back(
      {"numeric_add",
       "$add",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_add_fn_name}}});
  functions.push_back(
      {"numeric_sub",
       "$subtract",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_subtract_fn_name}}});
  functions.push_back(
      {"numeric_div",
       "$divide",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_divide_fn_name}}});
  functions.push_back(
      {"numeric_mul",
       "$multiply",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_multiply_fn_name}}});
  functions.push_back(
      {"numeric_abs",
       "abs",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_abs_fn_name}}});
  functions.push_back(
      {"numeric_mod",
       "mod",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_mod_fn_name}}});
  functions.push_back({"numeric_div_trunc",
                       "div",
                       {{{gsql_pg_numeric,
                          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_div_trunc_fn_name}}});
  functions.push_back(
      {"div",
       "div",
       {{{gsql_pg_numeric,
          {gsql_pg_numeric, gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_div_trunc_fn_name}}});
  functions.push_back(
      {"numeric_uminus",
       "$unary_minus",
       {{{gsql_pg_numeric, {gsql_pg_numeric},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/emulator_uminus_fn_name}}});

  // Basic comparison functions
  functions.push_back({"numeric_eq",
                       "$equal",
                       {{{gsql_bool,
                          {gsql_pg_numeric, gsql_pg_numeric},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"numeric_ne",
                       "$not_equal",
                       {{{gsql_bool,
                          {gsql_pg_numeric, gsql_pg_numeric},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"numeric_lt",
                       "$less",
                       {{{gsql_bool,
                          {gsql_pg_numeric, gsql_pg_numeric},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"numeric_le",
                       "$less_or_equal",
                       {{{gsql_bool,
                          {gsql_pg_numeric, gsql_pg_numeric},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"numeric_ge",
                       "$greater_or_equal",
                       {{{gsql_bool,
                          {gsql_pg_numeric, gsql_pg_numeric},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"numeric_gt",
                       "$greater",
                       {{{gsql_bool,
                          {gsql_pg_numeric, gsql_pg_numeric},
                          /*context_ptr=*/nullptr}}}});

  constexpr zetasql::Function::Mode AGGREGATE =
      zetasql::Function::AGGREGATE;

  // Remove existing function registration for sum.
  functions.erase(std::remove_if(functions.begin(), functions.end(),
                                 [](const PostgresFunctionArguments& args) {
                                   return args.postgres_function_name() ==
                                          "sum";
                                 }),
                  functions.end());
  // Register new function mapping for sum.
  functions.push_back(
      {"sum",
       "pg.sum",
       {{{gsql_pg_numeric, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}},
        {{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}},
       AGGREGATE});

  // Remove existing function registration for avg.
  functions.erase(std::remove_if(functions.begin(), functions.end(),
                                 [](const PostgresFunctionArguments& args) {
                                   return args.postgres_function_name() ==
                                          "avg";
                                 }),
                  functions.end());
  functions.push_back(
      {"avg",
       "pg.avg",
       {{{gsql_pg_numeric, {gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}},
        {{gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}}},
       AGGREGATE});
}

void AddPgNumericFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgNumericSignaturesForExistingFunctions(functions);
  AddPgNumericNewFunctions(functions);
}

static void AddPgJsonbSignaturesForExistingFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_jsonb =
      types::PgJsonbMapping()->mapped_type();
  const zetasql::Type* gsql_pg_jsonb_arr = nullptr;
  ABSL_CHECK_OK(
      GetTypeFactory()->MakeArrayType(gsql_pg_jsonb, &gsql_pg_jsonb_arr));

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
  const zetasql::Type* gsql_pg_numeric_arr = nullptr;
  ABSL_CHECK_OK(
      GetTypeFactory()->MakeArrayType(gsql_pg_numeric, &gsql_pg_numeric_arr));
  const zetasql::Type* gsql_pg_jsonb_arr = nullptr;
  ABSL_CHECK_OK(GetTypeFactory()->MakeArrayType(gsql_pg_jsonb, &gsql_pg_jsonb_arr));

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
        {{gsql_pg_jsonb, {gsql_int64_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_string_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_bool_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_double_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_bytes_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_timestamp_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_date_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_numeric_arr}, /*context_ptr=*/nullptr}},
        {{gsql_pg_jsonb, {gsql_pg_jsonb_arr}, /*context_ptr=*/nullptr}}}});

  std::string emulator_jsonb_typeof_fn_name = "pg.jsonb_typeof";
  std::string emulator_jsonb_array_element_fn_name = "pg.jsonb_array_element";
  std::string emulator_jsonb_object_field_fn_name = "pg.jsonb_object_field";

  functions.push_back({"jsonb_typeof",
                       "json_type",
                       {{{gsql_string,
                          {gsql_pg_jsonb},
                          /*context_ptr=*/nullptr},
                         /*has_postgres_proc_oid=*/true,
                         /*has_mapped_function=*/true,
                         /*explicit_mapped_function_name=*/
                         emulator_jsonb_typeof_fn_name}}});

  // Register new function mapping for '->' operator on JSONB arrays.
  functions.push_back({"jsonb_array_element",
                       "$subscript",
                       {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_int64},
                          /*context_ptr=*/nullptr},
                         /*has_postgres_proc_oid=*/true,
                         /*has_mapped_function=*/true,
                         /*explicit_mapped_function_name=*/
                         emulator_jsonb_array_element_fn_name}}});

  // Register new function mapping for '->' operator on JSONB objects.
  functions.push_back({"jsonb_object_field",
                       "$subscript",
                       {{{gsql_pg_jsonb,
                          {gsql_pg_jsonb, gsql_string},
                          /*context_ptr=*/nullptr},
                         /*has_postgres_proc_oid=*/true,
                         /*has_mapped_function=*/true,
                         /*explicit_mapped_function_name=*/
                         emulator_jsonb_object_field_fn_name}}});

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
}

void AddPgJsonbFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddPgJsonbSignaturesForExistingFunctions(functions);
  AddPgJsonbNewFunctions(functions);
}

void AddPgArrayFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_jsonb =
      types::PgJsonbMapping()->mapped_type();
  const zetasql::Type* gsql_pg_jsonb_arr = nullptr;
  ABSL_CHECK_OK(
      GetTypeFactory()->MakeArrayType(gsql_pg_jsonb, &gsql_pg_jsonb_arr));
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();
  const zetasql::Type* gsql_pg_numeric_arr = nullptr;
  ABSL_CHECK_OK(
      GetTypeFactory()->MakeArrayType(gsql_pg_numeric, &gsql_pg_numeric_arr));
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
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.array_upper"});

    array_upper_function.add_signature(
        {{gsql_int64,
          {gsql_pg_jsonb_arr, gsql_int64},
          /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
         /*has_mapped_function=*/true,
         /*explicit_mapped_function_name=*/"pg.array_upper"});
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

    const zetasql::Type* gsql_pg_numeric =
        types::PgNumericMapping()->mapped_type();
    functions.push_back({"to_number",
                         "pg.to_number",
                         {{{gsql_pg_numeric,
                            {gsql_string, gsql_string},
                            /*context_ptr=*/nullptr}}}});
    to_char_function.add_signature(
        {{gsql_string, {gsql_pg_numeric, gsql_string}, /*context_ptr=*/nullptr},
         /*has_postgres_proc_oid=*/true,
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
