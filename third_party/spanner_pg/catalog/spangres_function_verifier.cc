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

#include "third_party/spanner_pg/catalog/spangres_function_verifier.h"

#include <string>
#include <vector>

#include "zetasql/public/function.pb.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

namespace {
absl::Status CheckAllSignaturesHaveNamespacedPaths(
    const FunctionProto& function) {
  std::string name =
      absl::StrJoin(function.mapped_name_path().name_path(), ".");
  for (int i = 0; i < function.signatures_size(); ++i) {
    const FunctionSignatureProto& signature = function.signatures(i);
    for (const auto& pg_name_path : signature.postgresql_name_paths()) {
      ZETASQL_RET_CHECK(pg_name_path.name_path().size() == 2)
          << "Function " << name << " signature[" << i
          << "] postgresql_name_path must have 2 parts (namespace and name), "
             "got: "
          << absl::StrJoin(pg_name_path.name_path(), ".");
    }
  }
  return absl::OkStatus();
}

absl::Status CheckAtLeastOneSignature(const FunctionProto& function) {
  std::string name =
      absl::StrJoin(function.mapped_name_path().name_path(), ".");
  ZETASQL_RET_CHECK(function.signatures_size() > 0)
      << "Function " << name << " must have at least one signature";
  return absl::OkStatus();
}

absl::Status CheckAllNamedArgumentsHaveName(const FunctionProto& function) {
  std::string name =
      absl::StrJoin(function.mapped_name_path().name_path(), ".");
  for (int i = 0; i < function.signatures_size(); ++i) {
    const FunctionSignatureProto& signature = function.signatures(i);
    for (int j = 0; j < signature.arguments_size(); ++j) {
      const FunctionArgumentProto& arg = signature.arguments(j);
      zetasql::FunctionEnums::NamedArgumentKind named_kind =
          arg.named_argument_kind();
      if (named_kind == zetasql::FunctionEnums::POSITIONAL_OR_NAMED ||
          named_kind == zetasql::FunctionEnums::NAMED_ONLY) {
        ZETASQL_RET_CHECK(!arg.name().empty())
            << "Function " << name << " signature[" << i << "].arguments[" << j
            << "].name must be defined, since the named argument kind is set "
               "to "
            << zetasql::FunctionEnums::NamedArgumentKind_Name(named_kind);
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ValidateCatalogFunctions(
    const std::vector<FunctionProto>& functions) {
  for (const auto& function : functions) {
    ZETASQL_RETURN_IF_ERROR(CheckAtLeastOneSignature(function));
    ZETASQL_RETURN_IF_ERROR(CheckAllSignaturesHaveNamespacedPaths(function));
    ZETASQL_RETURN_IF_ERROR(CheckAllNamedArgumentsHaveName(function));
  }

  return absl::OkStatus();
}

}  // namespace postgres_translator
