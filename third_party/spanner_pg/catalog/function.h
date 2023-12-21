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

#ifndef CATALOG_FUNCTION_H_
#define CATALOG_FUNCTION_H_

#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

class PostgresExtendedFunction;
class PostgresExtendedFunctionSignature;

inline constexpr char kPostgresGroup[] = "pg";

// Group a function and signature together for the EngineSystemCatalog to return
// when asked how to execute a PostgreSQL function call in this storage engine.
struct FunctionAndSignature {
 public:
  FunctionAndSignature(const zetasql::Function* function,
                       const zetasql::FunctionSignature signature)
      : function_(function), signature_(signature) {}

  const zetasql::Function* function() const { return function_; }
  const zetasql::FunctionSignature& signature() const { return signature_; }

 private:
  const zetasql::Function* function_;
  const zetasql::FunctionSignature signature_;
};

// PostgresExtendedFunctionSignature identifies the argument types per
// overload of a Function.
// A PostgresExtendedFunctionSignature will typically have a PostgreSQL proc oid
// which it represents and a builtin function whose implementation it will
// use in the storage engine execution.
// In rare cases where the PostgreSQL proc return type is unsupported, the
// PostgresExtendedFunction will have two PostgresExtendedFunctionSignatures.
// One will map to the PostgreSQL proc with the unsupported return type and
// the other will map to a builtin function with the same input types but a
// different return type which is supported.
class PostgresExtendedFunctionSignature : public zetasql::FunctionSignature {
 public:
  // Constructs the function signature and stores metadata about the mapped
  // builtin function and mapped PostgreSQL proc oid.
  // If mapped_function has SignatureMatch value kDoesNotMatch, there is no
  // mapped builtin function. If there is a mapped builtin function,
  // mapped_function contians a copy of the builtin function with only one
  // signature that has the same input and output types as this signature.
  // If postgres_proc_oid is InvalidOid, there is no mapped PostgreSQL proc oid.
  // If postgres_proc_oid is a valid Oid, it must have the same input and output
  // types as this signature.
  PostgresExtendedFunctionSignature(
      const zetasql::FunctionSignature& signature,
      std::unique_ptr<zetasql::Function> mapped_function = nullptr,
      Oid postgres_proc_oid = InvalidOid)
      : zetasql::FunctionSignature(signature),
        mapped_function_(std::move(mapped_function)),
        postgres_proc_oid_(postgres_proc_oid) {}

  const zetasql::Function* mapped_function() const {
    return mapped_function_.get();
  }

  // Return an InvalidOid if there is no mapped PostgreSQL proc oid.
  Oid postgres_proc_oid() const { return postgres_proc_oid_; }

 private:
  std::unique_ptr<zetasql::Function> mapped_function_;
  Oid postgres_proc_oid_;
};

class PostgresExtendedFunction : public zetasql::Function {
 public:
  // Initialize a PostgresExtendedFunction.
  explicit PostgresExtendedFunction(
      const std::string& name, zetasql::Function::Mode mode,
      std::vector<std::unique_ptr<PostgresExtendedFunctionSignature>>
          function_signatures)
      : zetasql::Function(name, kPostgresGroup, mode),
        postgres_signatures_(std::move(function_signatures)) {
    for (const std::unique_ptr<PostgresExtendedFunctionSignature>& signature :
         postgres_signatures_) {
      // Call the zetasql::Function AddSignature function so all of the base
      // class lookups work correctly.
      AddSignature(*signature);

      // Store the oid to signature mapping.
      if (signature->postgres_proc_oid() != InvalidOid) {
        oid_to_signatures[signature->postgres_proc_oid()].push_back(
            signature.get());
      }
    }
  }

  static absl::Status UnsupportedFunctionError(absl::string_view function_name,
                                               absl::string_view arg_types) {
    return absl::UnimplementedError(absl::StrCat("Postgres function ",
                                                 function_name, "(", arg_types,
                                                 ") is not supported"));
  }

  const std::vector<std::unique_ptr<PostgresExtendedFunctionSignature>>&
  GetPostgresSignatures() const {
    return postgres_signatures_;
  }

  const PostgresExtendedFunctionSignature* GetPostgresSignature(int idx) const {
    if (idx < 0 || idx >= NumSignatures()) {
      return nullptr;
    }

    return postgres_signatures_[idx].get();
  }

  // Find the function signatures that match the PostgreSQL proc oid.
  // Return an error if there is no matching signature.
  absl::StatusOr<const std::vector<PostgresExtendedFunctionSignature*>>
  GetSignaturesForOid(Oid oid, absl::string_view input_type_list) const {
    auto it = oid_to_signatures.find(oid);
    if (it != oid_to_signatures.end()) {
      return it->second;
    } else {
      return UnsupportedFunctionError(Name(), input_type_list);
    }
  }

 private:
  std::vector<std::unique_ptr<PostgresExtendedFunctionSignature>>
      postgres_signatures_;
  absl::flat_hash_map<Oid, std::vector<PostgresExtendedFunctionSignature*>>
      oid_to_signatures;
};

}  // namespace postgres_translator
#endif  // CATALOG_FUNCTION_H_
