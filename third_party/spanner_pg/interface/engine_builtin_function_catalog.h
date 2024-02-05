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

#ifndef INTERFACE_ENGINE_BUILTIN_FUNCTION_CATALOG_
#define INTERFACE_ENGINE_BUILTIN_FUNCTION_CATALOG_

#include <string>

#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/statusor.h"

namespace postgres_translator {

// An abstract class for the builtin functions of a storage engine.
// Each storage engine should create a derived class which implements
// GetFunction.
class EngineBuiltinFunctionCatalog {
 public:
  // TODO: deprecate constructor with language options.
  explicit EngineBuiltinFunctionCatalog(
      const zetasql::LanguageOptions& /*language_options*/) {}

  EngineBuiltinFunctionCatalog() = default;

  virtual ~EngineBuiltinFunctionCatalog() {}

  // Looks up the function by name.
  // Returns a nullptr if the function is not found.
  virtual absl::StatusOr<const zetasql::Function*> GetFunction(
      const std::string& name) const = 0;

  virtual absl::StatusOr<const zetasql::TableValuedFunction*>
  GetTableValuedFunction(const std::string& name) const = 0;

  // Gets the full list of functions in the catalog.
  // TODO: switch this to pure virtual when the on-branch
  // SpangresBuiltinFunctionCatalog overrides it.
  virtual absl::Status GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const {
    return absl::OkStatus();
  }

  zetasql::TypeFactory* type_factory() { return &type_factory_; }

 private:
  zetasql::TypeFactory type_factory_;
};

}  // namespace postgres_translator

#endif  // INTERFACE_ENGINE_BUILTIN_FUNCTION_CATALOG_
