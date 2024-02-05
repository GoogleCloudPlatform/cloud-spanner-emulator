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

// Includes all string functions that are supported in both ZetaSQL and
// PostgreSQL. Any functions that are not supported in a specific storage engine
// like Spanner must be excluded by that storage engine's EngineSystemCatalog.
// See SpangresSystemCatalog::AddFunctions() for an example.
void AddStringFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_bool = zetasql::types::BoolType();
  const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  zetasql::FunctionArgumentTypeOptions repeated(
      zetasql::FunctionArgumentType::REPEATED);
  functions.push_back(
      {"textcat",
       "concat",
       {{{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  // Concat is a variadic function that is not validated when the
  // EngineSystemCatalog is initialized.
  functions.push_back({"concat",
                       "concat",
                       {{{gsql_string,
                          {gsql_string, {gsql_string, repeated}},
                          /*context_ptr=*/nullptr},
                         /*has_mapped_function=*/true,
                         /*explicit_mapped_function_name=*/"",
                         F_CONCAT}}});
  functions.push_back(
      {"byteacat",
       "concat",
       {{{gsql_bytes, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"strpos",
       "strpos",
       {{{gsql_int64, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"lower",
       "lower",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"upper",
       "upper",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"length",
       "length",
       {{{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_int64, {gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"starts_with",
       "starts_with",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"substr",
                       "substr",
                       {{{gsql_string,
                          {gsql_string, gsql_int64},
                          /*context_ptr=*/nullptr}},
                        {{gsql_string,
                          {gsql_string, gsql_int64, gsql_int64},
                          /*context_ptr=*/nullptr}},
                        {{gsql_bytes,
                          {gsql_bytes, gsql_int64},
                          /*context_ptr=*/nullptr}},
                        {{gsql_bytes,
                          {gsql_bytes, gsql_int64, gsql_int64},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back({"substring",
                       "substr",
                       {{{gsql_string,
                          {gsql_string, gsql_int64},
                          /*context_ptr=*/nullptr}},
                        {{gsql_string,
                          {gsql_string, gsql_int64, gsql_int64},
                          /*context_ptr=*/nullptr}},
                        {{gsql_bytes,
                          {gsql_bytes, gsql_int64},
                          /*context_ptr=*/nullptr}},
                        {{gsql_bytes,
                          {gsql_bytes, gsql_int64, gsql_int64},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"btrim",
       "trim",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_bytes, {gsql_bytes, gsql_bytes}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"ltrim",
       "ltrim",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"rtrim",
       "rtrim",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
        {{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"lpad",
       "lpad",
       {{{gsql_string, {gsql_string, gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_string,
          {gsql_string, gsql_int64, gsql_string},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"rpad",
       "rpad",
       {{{gsql_string, {gsql_string, gsql_int64}, /*context_ptr=*/nullptr}},
        {{gsql_string,
          {gsql_string, gsql_int64, gsql_string},
          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"left",
       "left",
       {{{gsql_string, {gsql_string, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"right",
       "right",
       {{{gsql_string, {gsql_string, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"repeat",
       "repeat",
       {{{gsql_string, {gsql_string, gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"reverse",
       "reverse",
       {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"replace",
                       "replace",
                       {{{gsql_string,
                          {gsql_string, gsql_string, gsql_string},
                          /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"ascii",
       "ascii",
       {{{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"chr", "chr", {{{gsql_string, {gsql_int64}, /*context_ptr=*/nullptr}}}});
}

void AddRegexFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_bool = zetasql::types::BoolType();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  functions.push_back(
      {"textregexeq",
       "regexp_contains",
       {{{gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}});
  functions.push_back({"regexp_replace",
                       "regexp_replace",
                       {{{gsql_string,
                          {gsql_string, gsql_string, gsql_string},
                          /*context_ptr=*/nullptr}}}});
}

void AddProto3ConversionFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddJSONFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddContainsSubstrFunction(
    std::vector<PostgresFunctionArguments>& functions) {}

}  // namespace postgres_translator
