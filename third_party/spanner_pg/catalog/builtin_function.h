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

#ifndef CATALOG_BUILTIN_FUNCTION_H_
#define CATALOG_BUILTIN_FUNCTION_H_

#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

// This file contains classes and methods to easily add builtin functions
// to an EngineSystemCatalog.
// PostgresFunctionSignature and PostgresFunctionSignatureArguments are used
// to define a function in the EngineSystemCatalog.
// AddX methods group the standard builtin functions in the same way as
// ZetaSQL (see storage/googlesql/common/builtin_function_internal.h)
// Each EngineSystemCatalog should use the AddX methods to populate its
// collection of supported functions, add any storage engine specific
// functions to the collection, and then call
// EngineSystemCatalog::AddFunction on each function in the collection.

namespace postgres_translator {
// Helper struct used to group functions by their respective name paths.
struct NamePathKey {
  std::vector<std::string> mapped_name_path;
  std::vector<std::string> postgresql_name_path;
};

bool operator<(const NamePathKey& lhs, const NamePathKey& rhs);

// Helper class used by AddFunction to create a
// PostgresExtendedFunctionSignature.
// If has_postgres_proc_oid is true, AddFunction will verify the existence of
// a PostgreSQL proc with an identical signature and use the proc Oid when
// creating the PostgresExtendedFunctionSignature.
// If has_mapped_function is true, AddFunction will verify the
// existence of an identical FunctionSignature in the mapped builtin function.
class PostgresFunctionSignatureArguments {
 public:
  // Only explicitly set postgres proc oid for signatures that cannot be
  // validated (variadic signatures).
  // Otherwise, allow the EngineSystemCatalog to validate that the provided
  // signature is compatible with native PostgreSQL and ZetaSQL before
  // identifying the proc oid.
  PostgresFunctionSignatureArguments(
      const zetasql::FunctionSignature& signature,
      bool has_mapped_function = true,
      const std::string& explicit_mapped_function_name = "",
      Oid postgres_proc_oid = InvalidOid,
      std::vector<std::string> query_features_names = {})
      : signature_(signature),
        has_mapped_function_(has_mapped_function),
        explicit_mapped_function_name_(explicit_mapped_function_name),
        postgres_proc_oid_(postgres_proc_oid),
        query_features_names_(query_features_names) {}

  const zetasql::FunctionSignature& signature() const { return signature_; }
  bool has_mapped_function() const { return has_mapped_function_; }
  const std::string& explicit_mapped_function_name() const {
    return explicit_mapped_function_name_;
  }
  // Returns the postgres proc oid for unvalidated signatures, or InvalidOid
  // for validated signatures.
  Oid postgres_proc_oid() const { return postgres_proc_oid_; }

  const std::vector<std::string>& query_features_names() const {
    return query_features_names_;
  }

 private:
  zetasql::FunctionSignature signature_;
  bool has_mapped_function_;
  // If the mapped function name for this signature is different from the
  // general mapped function name in PostgresFunctionArguments.
  std::string explicit_mapped_function_name_;
  Oid postgres_proc_oid_;
  // The query feature names that must be enabled so that this signature is
  // enabled.
  std::vector<std::string> query_features_names_;
};

// Helper class used by AddFunction to create a PostgresExtendedFunction.
// If mapped_function_name is non-empty, AddFunction will verify the existence
// of a builtin function with that name and use the function when validating
// the signature arguments.
// If mapped_function_name is empty, this function has a base implementation
// in the storage engine and does not need to reuse a builtin function.
class PostgresFunctionArguments {
 public:
  // Most function mappings should use this constructor because the mapped
  // signatures will be validated when the EngineSystemCatalog is initialized.
  PostgresFunctionArguments(
      absl::string_view postgres_function_name,
      absl::string_view mapped_function_name,
      const std::vector<PostgresFunctionSignatureArguments>&
          signature_arguments,
      zetasql::Function::Mode mode = zetasql::Function::SCALAR,
      absl::string_view postgres_namespace = "pg_catalog",
      std::vector<std::string> query_features_names = {})
      : postgres_function_name_(postgres_function_name),
        mapped_function_name_(mapped_function_name),
        signature_arguments_(signature_arguments),
        mode_(mode),
        postgres_namespace_(postgres_namespace),
        query_features_names_(query_features_names) {}

  const std::string& postgres_function_name() const {
    return postgres_function_name_;
  }


  const std::string& mapped_function_name() const {
    return mapped_function_name_;
  }

  const std::vector<PostgresFunctionSignatureArguments>& signature_arguments()
      const {
    return signature_arguments_;
  }

  void add_signature(PostgresFunctionSignatureArguments signature) {
    signature_arguments_.push_back(signature);
  }

  zetasql::Function::Mode mode() const { return mode_; }

  const std::string& postgres_namespace() const { return postgres_namespace_; }

  const std::vector<std::string>& query_features_names() const {
    return query_features_names_;
  }

 private:
  std::string postgres_function_name_;
  std::string mapped_function_name_;
  std::vector<PostgresFunctionSignatureArguments> signature_arguments_;
  zetasql::Function::Mode mode_;
  std::string postgres_namespace_;
  // The query feature names that must be enabled so that this function is
  // enabled.
  std::vector<std::string> query_features_names_;
};

}  // namespace postgres_translator

#endif  // CATALOG_BUILTIN_FUNCTION_H_
