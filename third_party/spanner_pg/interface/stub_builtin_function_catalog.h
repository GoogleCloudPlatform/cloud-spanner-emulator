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

#ifndef INTERFACE_STUB_BUILTIN_FUNCTION_CATALOG_H_
#define INTERFACE_STUB_BUILTIN_FUNCTION_CATALOG_H_

#include <string>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"

namespace postgres_translator {

// Stub version of EngineBuiltinFunctionCatalog that holds all ZetaSQL builtin
// functions.
class StubBuiltinFunctionCatalog : public EngineBuiltinFunctionCatalog {
 public:
  explicit StubBuiltinFunctionCatalog(
      const zetasql::LanguageOptions& language_options)
      : EngineBuiltinFunctionCatalog() {
    zetasql::GetZetaSQLFunctions(type_factory(), language_options,
                                     &googlesql_builtin_functions_);
  }

  absl::StatusOr<const zetasql::Function*> GetFunction(
      const std::string& name) const override {
    auto it = googlesql_builtin_functions_.find(name);
    if (it != googlesql_builtin_functions_.end()) {
      return it->second.get();
    } else {
      return nullptr;
    }
  }

 private:
  // This is the full set of ZetaSQL built-in functions.
  // It is a superset of the built-in functions which are available through the
  // PostgreSQL interface.
  // Each PostgresExtendedFunction must have its own implementation in the
  // storage engine or map to a ZetaSQL function in this set.
  std::map<std::string, std::unique_ptr<zetasql::Function>>
      googlesql_builtin_functions_;
};

}  // namespace postgres_translator

#endif  // INTERFACE_STUB_BUILTIN_FUNCTION_CATALOG_H_
