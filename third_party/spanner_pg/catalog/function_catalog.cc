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

#include "third_party/spanner_pg/catalog/function_catalog.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/btree_map.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"

namespace postgres_translator {

namespace {

using FnMap =
    absl::btree_map<std::string, std::unique_ptr<zetasql::Function>>;

inline const char kPostgresGroup[] = "pg";

const zetasql::Type* GsqlInt64() { return zetasql::types::Int64Type(); }
const zetasql::Type* GsqlDouble() { return zetasql::types::DoubleType(); }

std::unique_ptr<zetasql::Function> CreateFunction(
    const std::string& name,
    const std::vector<zetasql::FunctionSignature>& signatures) {
  return std::make_unique<zetasql::Function>(
      name, kPostgresGroup, zetasql::Function::SCALAR, signatures);
}

// Add all the Postgres functions to the catalog.
const FnMap* InitializePostgresFunctions() {
  // The key is the name of the function.
  // The constructor for FunctionSignature takes a return type, a list of
  // argument types, and a context pointer.
  absl::btree_map<std::string, std::vector<zetasql::FunctionSignature>>
      function_signatures = {
          {"abs",
           {{GsqlInt64(), {GsqlInt64()}, /*context_ptr=*/nullptr},
            {GsqlDouble(), {GsqlDouble()}, /*context_ptr=*/nullptr}}},
          {"float8", {{GsqlDouble(), {GsqlInt64()}, /*context_ptr=*/nullptr}}},
      };
  // Turn the signature list into an actual zetasql::Function.
  // We allocate the function map on the heap to have safe initialization
  // without destruction for the postgres_functions global object.
  FnMap* functions = new FnMap;
  for (const auto& [name, signatures] : function_signatures) {
    functions->emplace(name, CreateFunction(name, signatures));
  }
  return functions;
}
}  // namespace

absl::StatusOr<const zetasql::Function*> GetPostgresFunction(
    const std::string& name) {
  static const FnMap* postgres_functions = InitializePostgresFunctions();
  auto it = postgres_functions->find(absl::AsciiStrToLower(name));
  if (it != postgres_functions->end()) {
    return it->second.get();
  } else {
    return nullptr;
  }
}

}  // namespace postgres_translator
