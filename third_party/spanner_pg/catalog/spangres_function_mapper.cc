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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/codegen/postgresql_catalog.pb.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_type_d.h"
#include "third_party/spanner_pg/src/include/postgres_ext.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

namespace {

absl::Status CheckTypeAndKindMapping(
    uint32_t oid, const zetasql::Type* type,
    const zetasql::SignatureArgumentKind kind) {
  if (kind == zetasql::SignatureArgumentKind::ARG_TYPE_FIXED) {
    ZETASQL_RET_CHECK(type != nullptr) << "Type with OID " << oid
                               << " not found in the catalog (ARG_TYPE_FIXED "
                                  "and zetasql::Type* mapping is NULL)";
  }
  return absl::OkStatus();
}

// The default namespace is "pg_catalog", but it is specified as "pg" on
// function postgresql name paths. We perform the translation here.
std::string_view PostgresNamespaceFrom(std::string_view nspace) {
  return nspace == "pg" ? "pg_catalog" : nspace;
}

zetasql::SignatureArgumentKind SignatureArgumentKindFrom(uint32_t oid) {
  switch (oid) {
    case ANYOID:
      return zetasql::SignatureArgumentKind::ARG_TYPE_ARBITRARY;
    case ANYARRAYOID:
      return zetasql::SignatureArgumentKind::ARG_ARRAY_TYPE_ANY_1;
    case ANYELEMENTOID:
      return zetasql::SignatureArgumentKind::ARG_TYPE_ANY_1;
    default:
      return zetasql::SignatureArgumentKind::ARG_TYPE_FIXED;
  }
}

zetasql::FunctionSignature FunctionSignatureFrom(
    zetasql::FunctionArgumentType gsql_return_type,
    zetasql::FunctionArgumentTypeList gsql_arguments, bool is_deprecated) {
  if (is_deprecated) {
    zetasql::FunctionSignatureOptions options;
    options.set_is_deprecated(true);
    return zetasql::FunctionSignature(gsql_return_type, gsql_arguments,
                                        /*context_id=*/0, options);
  } else {
    return zetasql::FunctionSignature(gsql_return_type, gsql_arguments,
                                        /*context_ptr=*/nullptr);
  }
}

std::vector<std::string> FromProto(FunctionNamePathProto proto) {
  return std::vector<std::string>(proto.name_path().begin(),
                                  proto.name_path().end());
}

}  // namespace

const zetasql::Type* SpangresFunctionMapper::FindTypeByOid(
    uint32_t oid) const {
  const PostgresTypeMapping* type_mapping = catalog_->GetType(oid);

  return type_mapping != nullptr ? type_mapping->mapped_type() : nullptr;
}

absl::StatusOr<zetasql::FunctionArgumentType>
SpangresFunctionMapper::FunctionArgumentTypeFrom(
    ArgumentTypeProto arg_type) const {
  const zetasql::Type* type = FindTypeByOid(arg_type.oid());
  ZETASQL_RET_CHECK(type != nullptr)
      << "Type with OID " << arg_type.oid() << " not found in the catalog";

  return zetasql::FunctionArgumentType(type);
}

absl::StatusOr<zetasql::FunctionArgumentType>
SpangresFunctionMapper::FunctionArgumentTypeFrom(
    FunctionArgumentProto arg) const {
  uint32_t oid = arg.type().oid();
  const zetasql::Type* type = FindTypeByOid(oid);
  zetasql::SignatureArgumentKind kind = SignatureArgumentKindFrom(oid);
  ZETASQL_RETURN_IF_ERROR(CheckTypeAndKindMapping(oid, type, kind));

  zetasql::FunctionArgumentTypeOptions options;

  zetasql::FunctionEnums::NamedArgumentKind named_kind =
      arg.named_argument_kind();
  options.set_cardinality(arg.cardinality());

  if (arg.has_name()) {
    std::string name = arg.name();
    options.set_argument_name(name, named_kind);
  }

  if (type != nullptr) {
    return zetasql::FunctionArgumentType(type, options);
  } else {
    return zetasql::FunctionArgumentType(kind, options);
  }
}

absl::StatusOr<std::vector<PostgresFunctionArguments>>
SpangresFunctionMapper::ToPostgresFunctionArguments(
    const FunctionProto& function) const {
  std::vector<PostgresFunctionArguments> result;

  // Assumes function has been formatted to a single postgresql name path in all
  // signatures and it is the same as the function's postgresql name path
  ZETASQL_RET_CHECK(function.postgresql_name_paths_size() == 1);
  for (const auto& signature : function.signatures()) {
    ZETASQL_RET_CHECK(signature.postgresql_name_paths_size() == 1);
    ZETASQL_RET_CHECK(FromProto(signature.postgresql_name_paths()[0]) ==
              FromProto(function.postgresql_name_paths()[0]));
  }

  std::vector<std::string> mapped_name_path =
      FromProto(function.mapped_name_path());
  // Guaranteed by the assumptions above
  std::vector<std::string> postgresql_name_path =
      FromProto(function.postgresql_name_paths()[0]);

  std::vector<PostgresFunctionSignatureArguments> pg_signatures;
  for (const auto& signature : function.signatures()) {
    ZETASQL_ASSIGN_OR_RETURN(zetasql::FunctionArgumentType gsql_return_type,
                     FunctionArgumentTypeFrom(signature.return_type()));

    zetasql::FunctionArgumentTypeList gsql_arguments;
    for (const auto& argument : signature.arguments()) {
      ZETASQL_ASSIGN_OR_RETURN(zetasql::FunctionArgumentType gsql_arg_type,
                       FunctionArgumentTypeFrom(argument));
      gsql_arguments.push_back(gsql_arg_type);
    }

    zetasql::FunctionSignature gsql_signature = FunctionSignatureFrom(
        gsql_return_type, gsql_arguments, signature.deprecated());

    Oid signature_oid =
        signature.has_oid() ? signature.oid() : InvalidOid;  // NOLINT
    pg_signatures.push_back(PostgresFunctionSignatureArguments(
        gsql_signature,
        /*has_mapped_function=*/true,
        /*explicit_mapped_function_name=*/"", signature_oid));
  }

  std::string mapped_function_name = absl::StrJoin(mapped_name_path, ".");
  result.push_back(PostgresFunctionArguments(
      postgresql_name_path[1], mapped_function_name, pg_signatures,
      zetasql::Function::SCALAR,  // Only Scalar functions are supported
      PostgresNamespaceFrom(postgresql_name_path[0])));

  return result;
}

}  // namespace postgres_translator
