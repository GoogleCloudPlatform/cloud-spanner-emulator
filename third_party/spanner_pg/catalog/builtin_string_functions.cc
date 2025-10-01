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

#include "zetasql/public/types/type.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"

namespace postgres_translator {

// Includes all string functions that are supported in both ZetaSQL and
// PostgreSQL. Any functions that are not supported in a specific storage engine
// like Spanner must be excluded by that storage engine's EngineSystemCatalog.
// See SpangresSystemCatalog::AddFunctions() for an example.
void AddStringFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
  const zetasql::Type* gsql_string = zetasql::types::StringType();
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
}

}  // namespace postgres_translator
