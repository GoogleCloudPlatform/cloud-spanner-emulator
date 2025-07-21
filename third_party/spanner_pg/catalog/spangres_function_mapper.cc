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

#include "third_party/spanner_pg/catalog/spangres_function_mapper.h"

#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/proto/catalog.pb.h"
#include "third_party/spanner_pg/src/include/postgres_ext.h"

namespace postgres_translator {

constexpr char kPgNamespace[] = "pg";
constexpr char kDefaultFunctionNamespace[] = "pg_catalog";

absl::StatusOr<PostgresFunctionArguments>
SpangresFunctionMapper::ToPostgresFunctionArguments(
    const FunctionProto& function) const {
  if (function.mapped_name_path().size() > 2) {
    return absl::InternalError(absl::StrFormat(
        "Unsupported function mapped name path with nested namespaces: %s",
        absl::StrJoin(function.mapped_name_path(), ".")));
  }
  if (function.postgresql_name_path().size() > 2) {
    return absl::InternalError(absl::StrFormat(
        "Unsupported function postgresql name path with nested namespaces: %s",
        absl::StrJoin(function.postgresql_name_path(), ".")));
  }

  std::string postgres_function_name =
      *function.postgresql_name_path().rbegin();
  std::string mapped_function_name =
      absl::StrJoin(function.mapped_name_path(), ".");
  std::string postgres_namespace = kDefaultFunctionNamespace;
  if (function.postgresql_name_path().size() > 1) {
    std::string function_namespace = function.postgresql_name_path()[0];
    // pg namespace is mapped to the default namespace
    if (function_namespace != kPgNamespace) {
      postgres_namespace = function_namespace;
    }
  }

  std::vector<PostgresFunctionSignatureArguments> signatures;
  for (const auto& signature : function.signatures()) {
    zetasql::FunctionArgumentType gsql_return_type(
        catalog_->GetType(signature.return_type().oid())->mapped_type());

    zetasql::FunctionArgumentTypeList gsql_arguments;
    for (const auto& argument : signature.arguments()) {
      gsql_arguments.push_back(
          catalog_->GetType(argument.type().oid())->mapped_type());
    }

    zetasql::FunctionSignature gsql_signature(gsql_return_type,
                                                gsql_arguments,
                                                /*context_ptr=*/nullptr);
    Oid signature_oid =
        signature.has_oid() ? signature.oid() : InvalidOid;  // NOLINT
    signatures.push_back(PostgresFunctionSignatureArguments(
        gsql_signature,
        /*has_mapped_function=*/true,
        /*explicit_mapped_function_name=*/"", signature_oid));
  }

  return PostgresFunctionArguments(
      postgres_function_name, mapped_function_name, signatures,
      zetasql::Function::SCALAR,  // Only Scalar functions are supported
      postgres_namespace);
}

}  // namespace postgres_translator
