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

#include "third_party/spanner_pg/catalog/builtin_scalar_functions.h"

#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/algorithm/container.h"
#include "absl/flags/flag.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"

namespace postgres_translator {

namespace {

const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_date = zetasql::types::DateType();
const zetasql::Type* gsql_int64_array = zetasql::types::Int64ArrayType();
const zetasql::Type* gsql_interval = zetasql::types::IntervalType();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_string_arr = zetasql::types::StringArrayType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();

void AddArrayFunctions(std::vector<PostgresFunctionArguments>& functions) {
  // TODO: b/446759597 - Automate registration of array_to_string
  // Should be <required>, <required>, <optional>, but we don't support
  // optional. Workaround is to put in both variants as separate signatures.
  // Note: byte array is supported by googlesql but intentionally omitted
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

void AddDateFunctions(std::vector<PostgresFunctionArguments>& functions) {
  // TODO: b/446758553 - Automate registration of date function
  functions.push_back({"make_date",
                       "date",
                       {{{gsql_date,
                          {gsql_int64, gsql_int64, gsql_int64},
                          /*context_ptr=*/nullptr}}}});

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

void AddFormattingFunctions(std::vector<PostgresFunctionArguments>& functions) {
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

void AddMathematicalFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_pg_numeric =
      spangres::types::PgNumericMapping()->mapped_type();

  {
      auto extract_function =
          absl::c_find_if(functions, [](const PostgresFunctionArguments& args) {
            return (args.postgres_function_name() == "extract" &&
                    args.postgres_namespace() == "pg_catalog");
          });
      if (extract_function == functions.end()) {
        functions.push_back({"extract",
                             "pg.extract_interval",
                             {{{gsql_pg_numeric,
                                {gsql_string, gsql_interval},
                                /*context_ptr=*/nullptr}}}});
      } else {
        extract_function->add_signature(
            {{gsql_pg_numeric,
              {gsql_string, gsql_interval},
              /*context_ptr=*/nullptr},
             /*has_mapped_function=*/true,
             /*explicit_mapped_function_name=*/"pg.extract_interval"});
      }
  }
}

void AddStringFunctions(std::vector<PostgresFunctionArguments>& functions) {
  zetasql::FunctionArgumentTypeOptions repeated(
      zetasql::FunctionArgumentType::REPEATED);
  // TODO: b/446760044 - Automate registration of textcat
  functions.push_back(
      {"textcat",
       "concat",
       {{{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  // TODO: b/446760044 - Automate registration of byteacat
  functions.push_back(
      {"byteacat",
       "concat",
       {{{gsql_bytes, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});

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

}  // namespace

void AddScalarFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddArrayFunctions(functions);
  AddDateFunctions(functions);
  AddFormattingFunctions(functions);
  AddMathematicalFunctions(functions);
  AddStringFunctions(functions);
}

}  // namespace postgres_translator
