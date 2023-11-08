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

#ifndef INTERFACE_EMULATOR_BUILTIN_FUNCTION_CATALOG_
#define INTERFACE_EMULATOR_BUILTIN_FUNCTION_CATALOG_

#include <string>

#include "zetasql/public/function.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/statusor.h"
#include "backend/query/function_catalog.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace spangres {

// A wrapper around the Emulator catalog so that PG Spanner can access the
// functions that are built into Cloud Spanner.
class EmulatorBuiltinFunctionCatalog : public EngineBuiltinFunctionCatalog {
 public:
  explicit EmulatorBuiltinFunctionCatalog(zetasql::TypeFactory* type_factory)
      : EngineBuiltinFunctionCatalog(),
        function_catalog_(type_factory) {
  }

  explicit EmulatorBuiltinFunctionCatalog(zetasql::TypeFactory* type_factory,
                                          const std::string& catalog_name)
      : EngineBuiltinFunctionCatalog(),
        function_catalog_(type_factory, catalog_name) {
  }

  ~EmulatorBuiltinFunctionCatalog() {
  }

  absl::StatusOr<const zetasql::Function*> GetFunction(
      const std::string& name) const override {
    const zetasql::Function* function;
    function_catalog_.GetFunction(name, &function);
    if (function == nullptr) {
      return absl::NotFoundError(absl::StrCat(name, " function not found"));
    }
    return function;
  }

  absl::Status GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const override {
    ZETASQL_RET_CHECK_NE(output, nullptr);
    ZETASQL_RET_CHECK(output->empty());
    function_catalog_.GetFunctions(output);
    return absl::OkStatus();
  }

 private:
  google::spanner::emulator::backend::FunctionCatalog function_catalog_;
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // INTERFACE_EMULATOR_BUILTIN_FUNCTION_CATALOG_
