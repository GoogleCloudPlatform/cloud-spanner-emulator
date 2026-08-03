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

#include "third_party/spanner_pg/catalog/engine_system_catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/analyzer/function_signature_matcher.h"
#include "googlesql/parser/ast_node.h"
#include "googlesql/public/cast.h"
#include "googlesql/public/coercer.h"
#include "googlesql/public/function.pb.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/input_argument_type.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/procedure.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/type.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "googlesql/public/builtin_function.h"
#include "googlesql/public/builtin_function_options.h"

namespace postgres_translator {

static bool TypesMatch(const googlesql::Type* type1,
                       const googlesql::Type* type2) {
  if (type1 == type2) {
    return true;
  }

  // The types may be null for templated types.
  if (type1 == nullptr || type2 == nullptr) {
    return false;
  }

  // The same type might be instantiated by two different TypeFactories and
  // have two different memory pointers.
  if (type1->IsArray() && type2->IsArray() &&
      type1->AsArray()->element_type() == type2->AsArray()->element_type()) {
    return true;
  }
  return false;
}

absl::StatusOr<std::unique_ptr<googlesql::Function>>
EngineSystemCatalog::BuildMappedFunction(
    const googlesql::FunctionSignature& postgres_signature,
    const std::vector<googlesql::InputArgumentType>&
        postgres_input_argument_types,
    const googlesql::Function* mapped_function,
    const googlesql::LanguageOptions& language_options) {
  // Verify that there is a matching builtin signature.
  GOOGLESQL_RET_CHECK_NE(mapped_function, nullptr);
  for (const googlesql::FunctionSignature& googlesql_signature :
       mapped_function->signatures()) {
    if (googlesql_signature.result_type().kind() ==
            googlesql::ARG_KIND_EXPR_FIXED &&
        !TypesMatch(googlesql_signature.result_type().type(),
                    postgres_signature.result_type().type())) {
      // The return type is specified and does not match.
      continue;
    }

    std::unique_ptr<googlesql::FunctionSignature> result_signature;
    if (SignatureMatches(postgres_input_argument_types, googlesql_signature,
                         &result_signature, language_options)) {
      // Create and return the copied function.
      return std::make_unique<googlesql::Function>(
          mapped_function->FunctionNamePath(), mapped_function->GetGroup(),
          mapped_function->mode(),
          std::vector<googlesql::FunctionSignature>{googlesql_signature},
          mapped_function->function_options());
    }
  }
  return absl::NotFoundError(absl::StrFormat(
      "Function %s does not have signature: %s.", mapped_function->FullName(),
      postgres_signature.DebugString(mapped_function->FullName())));
}

EngineSystemCatalog* EngineSystemCatalog::GetEngineSystemCatalog() {
  return spangres::SpangresSystemCatalog::GetSpangresSystemCatalog();
}

absl::Status EngineSystemCatalog::GetType(const std::string& name,
                                          const googlesql::Type** type,
                                          const FindOptions& options) {
  *type = GetType(name);
  return absl::OkStatus();
}

const PostgresTypeMapping* EngineSystemCatalog::GetType(
    const std::string& name) const {
  auto it = engine_types_.find(name);
  if (it != engine_types_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

const PostgresTypeMapping* EngineSystemCatalog::GetType(Oid oid) const {
  auto type_name_or = PgBootstrapCatalog::Default()->GetTypeName(oid);
  if (!type_name_or.ok()) {
    return nullptr;
  } else {
    return GetType(type_name_or.value());
  }
}

const PostgresTypeMapping* EngineSystemCatalog::GetTypeFromReverseMapping(
    const googlesql::Type* type, int max_length) const {
  ABSL_DCHECK_GE(max_length, 0);

  auto it = engine_types_reverse_map_.find(type);
  if (it != engine_types_reverse_map_.end()) {
    const PostgresTypeMapping* type = GetType(it->second);
    ABSL_DCHECK_NE(type, nullptr);
    return type;
  } else {
    return nullptr;
  }
}

absl::Status EngineSystemCatalog::GetFunction(
    const std::string& name, const googlesql::Function** function,
    const FindOptions& options) {
  std::vector<absl::string_view> name_path = absl::StrSplit(name, '.');
  absl::string_view namespace_name, function_name;
  if (name_path.size() > 2) {
    // PG doesn't support nested namespaces.
    *function = nullptr;
    return absl::OkStatus();
  } else if (name_path.size() == 2) {
    namespace_name = name_path[0];
    function_name = name_path[1];
  } else {
    namespace_name = "pg_catalog";
    function_name = name_path[0];
  }
  *function = GetFunction(namespace_name, function_name);
  return absl::OkStatus();
}

const PostgresExtendedFunction* EngineSystemCatalog::GetFunction(
    absl::string_view namespace_name, absl::string_view function_name) const {
  if (auto it = engine_functions_.find(std::make_pair(
          std::string(namespace_name), std::string(function_name)));
      it != engine_functions_.end()) {
    return it->second.get();
  } else {
    return nullptr;
  }
}

const googlesql::Function* EngineSystemCatalog::GetFunction(
    const PostgresExprIdentifier& expr_id) const {
  auto it = pg_expr_to_builtin_function_.find(expr_id);
  if (it != pg_expr_to_builtin_function_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

const googlesql::TableValuedFunction*
EngineSystemCatalog::GetTableValuedFunction(Oid proc_oid) const {
  auto it = proc_oid_to_tvf_.find(proc_oid);
  if (it != proc_oid_to_tvf_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

absl::StatusOr<Oid> EngineSystemCatalog::GetOidForTVF(
    const googlesql::TableValuedFunction* tvf) const {
  auto it = tvf_to_proc_oid_.find(tvf);
  if (it != tvf_to_proc_oid_.end()) {
    return it->second;
  }
  return absl::NotFoundError(
      absl::StrFormat("TableValuedFunction %s does not have a mapped proc oid.",
                      tvf->FullName()));
}

const googlesql::Procedure* EngineSystemCatalog::GetProcedure(
    Oid proc_oid) const {
  auto it = proc_oid_to_procedure_.find(proc_oid);
  if (it != proc_oid_to_procedure_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

bool EngineSystemCatalog::HasCastOverrideFunction(
    const googlesql::Type* source_type, const googlesql::Type* target_type) {
  std::pair<const googlesql::Type*, const googlesql::Type*> cast_pair(
      source_type, target_type);
  return pg_cast_to_builtin_function_.contains(cast_pair);
}

absl::StatusOr<FunctionAndSignature>
EngineSystemCatalog::GetCastOverrideFunctionAndSignature(
    const googlesql::Type* source_type, const googlesql::Type* target_type,
    const googlesql::LanguageOptions& language_options) {
  googlesql::ProductMode product_mode = language_options.product_mode();
  std::pair<const googlesql::Type*, const googlesql::Type*> cast_pair(
      source_type, target_type);
  auto it = pg_cast_to_builtin_function_.find(cast_pair);
  GOOGLESQL_RET_CHECK(it != pg_cast_to_builtin_function_.end()) << absl::StrFormat(
      "Unable to find cast override function from <%s> to <%s>",
      source_type->TypeName(product_mode), target_type->TypeName(product_mode));
  return it->second;
}

absl::StatusOr<FunctionAndSignature>
EngineSystemCatalog::GetFunctionAndSignature(
    Oid proc_oid,
    const std::vector<googlesql::InputArgumentType>& input_argument_types,
    const googlesql::LanguageOptions& language_options) {
  GOOGLESQL_ASSIGN_OR_RETURN(const struct FormData_pg_proc* proc,
                   PgBootstrapCatalog::Default()->GetProc(proc_oid));
  GOOGLESQL_RET_CHECK_NE(proc, nullptr);

  GOOGLESQL_ASSIGN_OR_RETURN(
      const char* namespace_name,
      PgBootstrapCatalog::Default()->GetNamespaceName(proc->pronamespace));
  GOOGLESQL_RET_CHECK_NE(namespace_name, nullptr);

  // This builds a string representation of the postgres input types
  // to be used in error messages.
  std::vector<absl::string_view> postgres_input_type_names;
  for (const googlesql::InputArgumentType& input_arg : input_argument_types) {
    const PostgresTypeMapping* pg_type =
        GetTypeFromReverseMapping(input_arg.type());

    // if we cannot reverse the input type, a null pointer is returned
    // and we skip adding input types in the error message.
    if (pg_type == nullptr) {
      postgres_input_type_names.clear();
      break;
    } else {
      GOOGLESQL_ASSIGN_OR_RETURN(const char* pg_type_name,
                       pg_type->PostgresExternalTypeName());
      postgres_input_type_names.push_back(pg_type_name);
    }
  }

  std::string postgres_input_args_string = absl::StrJoin(
      postgres_input_type_names.begin(), postgres_input_type_names.end(), ", ");

  const PostgresExtendedFunction* function =
      GetFunction(namespace_name, NameStr(proc->proname));
  if (function != nullptr) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        const std::vector<PostgresExtendedFunctionSignature*>& signatures,
        function->GetSignaturesForOid(proc_oid, postgres_input_args_string));

    for (const PostgresExtendedFunctionSignature* signature : signatures) {
      // Run the function signature matcher to be sure that the
      // EngineSystemCatalog signature is compatible with the input argument
      // types.
      std::unique_ptr<googlesql::FunctionSignature> result_signature;
      if (SignatureMatches(input_argument_types, *signature, &result_signature,
                           language_options)) {
        if (!signature->mapped_function()) {
          if (signature->options().rewrite_options() &&
              !signature->options().rewrite_options()->sql().empty()) {
            return FunctionAndSignature(function, *result_signature);
          }
          // If there isn't a mapped function and the signature has not defined
          // a rewrite in the engine system catalog, the signature is
          // unsupported and requires an explicit cast on the output.
          // TODO : support explicit casting on function output.
          return absl::UnimplementedError(absl::StrCat(
              "Postgres Function requires an explicit cast: ", "Namespace: ",
              namespace_name, "Name: ", NameStr(proc->proname), ", ",
              "Oid: ", proc_oid, ", ", "Arguments: ", "(",
              postgres_input_args_string, ")"));
        }

        for (const googlesql::FunctionSignature& mapped_signature :
          signature->mapped_function()->signatures()) {
          // Run the function signature matcher again to be sure that the
          // mapped builtin signature matches the input argument types.
          if (SignatureMatches(input_argument_types, mapped_signature,
                              &result_signature, language_options)) {
            // We return the result signature instead of the mapped signature
            // because the Function Signature Matcher fills in the actual types
            // if the original signature had ARG_KIND_EXPR_ANY_1 input or output
            // types.
            return FunctionAndSignature(signature->mapped_function(),
                                        *result_signature);
          }
        }
      }
    }
  }
  return PostgresExtendedFunction::UnsupportedFunctionError(
      NameStr(proc->proname), postgres_input_args_string);
}

absl::StatusOr<FunctionAndSignature>
EngineSystemCatalog::GetFunctionAndSignature(
    const PostgresExprIdentifier& expr_id,
    const std::vector<googlesql::InputArgumentType>& input_argument_types,
    const googlesql::LanguageOptions& language_options) {
  const googlesql::Function* builtin_function = GetFunction(expr_id);
  if (builtin_function != nullptr) {
    for (const googlesql::FunctionSignature& signature :
         builtin_function->signatures()) {
      // Run the function signature matcher to check if the builtin signature
      // matches the input argument types.
      std::unique_ptr<googlesql::FunctionSignature> result_signature;
      if (SignatureMatches(input_argument_types, signature, &result_signature,
                           language_options)) {
        // We return the result signature instead of the mapped signature
        // because the the Function Signature Matcher fills in the actual
        // types if the original signature had ARG_KIND_EXPR_ANY_1 input or
        // output types.
        return FunctionAndSignature(builtin_function, *result_signature);
      }
    }
  }
  std::vector<absl::string_view> postgres_input_type_names;
  for (const googlesql::InputArgumentType& input_arg : input_argument_types) {
    const PostgresTypeMapping* pg_type =
        GetTypeFromReverseMapping(input_arg.type());

    // if we cannot reverse the input type, a null pointer is returned
    // and we skip adding input types in the error message.
    if (pg_type == nullptr) {
      postgres_input_type_names.clear();
      break;
    } else {
      GOOGLESQL_ASSIGN_OR_RETURN(const char* pg_type_name,
                       pg_type->PostgresExternalTypeName());
      postgres_input_type_names.push_back(pg_type_name);
    }
  }

  std::string postgres_input_args_string = absl::StrJoin(
      postgres_input_type_names.begin(), postgres_input_type_names.end(), ", ");
  return absl::UnimplementedError(
      absl::StrCat("Unsupported Postgres expression. Expression type: ",
                   NodeTagToNodeString(expr_id.node_tag()), ", Arguments: (",
                   postgres_input_args_string, ")"));
}

absl::StatusOr<ProcedureAndSignature>
EngineSystemCatalog::GetProcedureAndSignature(
    Oid proc_oid,
    const std::vector<googlesql::InputArgumentType>& input_argument_types,
    const googlesql::LanguageOptions& language_options) {
  GOOGLESQL_ASSIGN_OR_RETURN(const FormData_pg_proc* proc,
                   PgBootstrapCatalog::Default()->GetProc(proc_oid));
  std::string full_proc_name = NameStr(proc->proname);
  if (proc->pronamespace != PG_CATALOG_NAMESPACE) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        const char* namespace_name,
        PgBootstrapCatalog::Default()->GetNamespaceName(proc->pronamespace));
    full_proc_name = absl::StrCat(namespace_name, ".", full_proc_name);
  }

  // This builds a string representation of the postgres input types
  // to be used in error messages.
  std::vector<absl::string_view> postgres_input_type_names;
  for (const googlesql::InputArgumentType& input_arg : input_argument_types) {
    const PostgresTypeMapping* pg_type =
        GetTypeFromReverseMapping(input_arg.type());

    // If we cannot reverse the input type, a null pointer is returned
    // and we skip adding input types in the error message.
    if (pg_type == nullptr) {
      postgres_input_type_names.clear();
      break;
    } else {
      GOOGLESQL_ASSIGN_OR_RETURN(const char* pg_type_name,
                       pg_type->PostgresExternalTypeName());
      postgres_input_type_names.push_back(pg_type_name);
    }
  }

  std::string postgres_input_args_string = absl::StrJoin(
      postgres_input_type_names.begin(), postgres_input_type_names.end(), ", ");

  const googlesql::Procedure* procedure = GetProcedure(proc_oid);
  if (procedure != nullptr) {
    std::unique_ptr<googlesql::FunctionSignature> result_signature;
    if (SignatureMatches(input_argument_types, procedure->signature(),
                         &result_signature, language_options)) {
      return ProcedureAndSignature(procedure, *result_signature);
    }
  }
  return PostgresExtendedFunction::UnsupportedProcedureError(
      full_proc_name, postgres_input_args_string);
}

absl::StatusOr<Oid> EngineSystemCatalog::GetPgProcOidFromReverseMapping(
    const std::string& function_name,
    const std::vector<googlesql::InputArgumentType>& input_argument_types,
    const googlesql::LanguageOptions& language_options) {
  // Check the operators first for a matching signature.
  auto it = engine_function_operators_reverse_map_.find(function_name);
  if (it != engine_function_operators_reverse_map_.end()) {
    for (Oid proc_candidate : it->second) {
      // Use the forward transformer lookup to check if the proc is valid.
      if (GetFunctionAndSignature(proc_candidate, input_argument_types,
                                  language_options)
              .ok()) {
        return proc_candidate;
      }
    }
  }

  // No matching operator function was found. Check the non-operator functions.
  it = engine_function_non_operators_reverse_map_.find(function_name);
  if (it != engine_function_non_operators_reverse_map_.end()) {
    for (Oid proc_candidate : it->second) {
      // Use the forward transformer lookup to check if the proc is valid.
      if (GetFunctionAndSignature(proc_candidate, input_argument_types,
                                  language_options)
              .ok()) {
        return proc_candidate;
      }
    }
  }

  return absl::UnimplementedError(
      absl::StrCat("No Postgres proc oid found for function ", function_name,
                   " with the provided argument types"));
}

bool EngineSystemCatalog::IsGsqlFunctionMappedToPgExpr(
    const std::string& function_name) {
  return engine_function_expr_reverse_map_.find(function_name) !=
         engine_function_expr_reverse_map_.end();
}

absl::StatusOr<PostgresExprIdentifier>
EngineSystemCatalog::GetPostgresExprIdentifier(
    const std::string& function_name) {
  GOOGLESQL_RET_CHECK(IsGsqlFunctionMappedToPgExpr(function_name));
  return engine_function_expr_reverse_map_.find(function_name)->second;
}

bool EngineSystemCatalog::IsGsqlFunctionMappedToPgCast(
    const std::string& function_name) {
  return engine_cast_functions_.contains(function_name);
}

absl::Status EngineSystemCatalog::GetTypes(
    absl::flat_hash_set<const googlesql::Type*>* output) const {
  GOOGLESQL_RET_CHECK_NE(output, nullptr);
  GOOGLESQL_RET_CHECK(output->empty());

  // Engines may include types that are unsupported.
  // These types may only be used in queries where they are explicitly cast to
  // a supported type.
  // For now, RQG only cares about the fully supported types.
  for (const auto& [type_name, type] : engine_types_) {
    if (type->IsSupportedType(googlesql::LanguageOptions())) {
      if (type->mapped_type() == nullptr) {
        output->insert(type);
      } else {
        output->insert(type->mapped_type());
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> EngineSystemCatalog::IsSetReturningFunction(
    Oid proc_oid) const {
  GOOGLESQL_ASSIGN_OR_RETURN(const FormData_pg_proc* proc,
                   PgBootstrapCatalog::Default()->GetProc(proc_oid));
  return proc->proretset;
}

absl::Status EngineSystemCatalog::GetFunctions(
    absl::flat_hash_set<const googlesql::Function*>* output) const {
  for (const auto& [_, function] : engine_functions_) {
    for (const std::unique_ptr<PostgresExtendedFunctionSignature>& signature :
         function->GetPostgresSignatures()) {
      // If there isn't a mapped function, the signature is unsupported and
      // should not be returned to RQG.
      if (signature->mapped_function() == nullptr) {
        continue;
      }
      GOOGLESQL_ASSIGN_OR_RETURN(bool is_set_returning_function,
                       IsSetReturningFunction(signature->postgres_proc_oid()));
      if (!is_set_returning_function) {
        output->insert(signature->mapped_function());
      }
    }
  }

  // Add the builtin functions that are mapped to PG Expr Identifiers.
  for (const auto& [expr_id, function] : pg_expr_to_builtin_function_) {
    output->insert(function);
  }

  // Add builtin functions for casting.
  for (const auto& [cast_pair, function] : pg_cast_to_builtin_function_) {
    output->insert(function.function());
  }

  return absl::OkStatus();
}

absl::StatusOr<std::vector<FunctionSigPair>>
EngineSystemCatalog::GetFunctionSigPairs(
    absl::string_view function_namespace,
    absl::string_view function_name) const {
  const PostgresExtendedFunction* pg_function =
      GetFunction(function_namespace, function_name);
  GOOGLESQL_RET_CHECK_NE(pg_function, nullptr) << "Function not found: " << function_name;
  std::vector<FunctionSigPair> signatures;
  for (const std::unique_ptr<PostgresExtendedFunctionSignature>& signature :
       pg_function->GetPostgresSignatures()) {
    GOOGLESQL_RET_CHECK_NE(signature->mapped_function(), nullptr);
    signatures.push_back({signature->mapped_function(), signature.get()});
  }
  return signatures;
}

absl::Status EngineSystemCatalog::GetSetReturningFunctions(
    absl::flat_hash_set<const googlesql::Function*>* output) const {
  for (const auto& [function_name, function] : engine_functions_) {
    for (const std::unique_ptr<PostgresExtendedFunctionSignature>& signature :
         function->GetPostgresSignatures()) {
      // If there isn't a mapped function, the signature is unsupported and
      // should not be returned to RQG.
      if (signature->mapped_function() == nullptr) {
        continue;
      }
      GOOGLESQL_ASSIGN_OR_RETURN(bool is_set_returning_function,
                       IsSetReturningFunction(signature->postgres_proc_oid()));
      if (is_set_returning_function) {
        output->insert(signature->mapped_function());
      }
    }
  }
  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::GetTableValuedFunctions(
    absl::flat_hash_map<Oid, const googlesql::TableValuedFunction*>* output)
    const {
  for (const auto& [tvf_oid, tvf] : proc_oid_to_tvf_) {
    output->insert({tvf_oid, tvf});
  }
  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::GetProcedures(
    absl::flat_hash_map<Oid, const googlesql::Procedure*>* output) const {
  for (const auto& [proc_oid, procedure] : proc_oid_to_procedure_) {
    output->insert({proc_oid, procedure});
  }
  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::GetPostgreSQLFunctions(
    absl::flat_hash_set<const PostgresExtendedFunction*>* output) const {
  for (const auto& [_, function] : engine_functions_) {
    output->insert(function.get());
  }

  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::GetPostgreSQLTypes(
    absl::flat_hash_set<const PostgresTypeMapping*>* output) const {
  for (const auto& [type_name, type_mapping] : engine_types_) {
    output->insert(type_mapping);
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> EngineSystemCatalog::IsValidCast(
    const googlesql::Type* from_type, const googlesql::Type* to_type,
    const googlesql::LanguageOptions& language_options) {
  googlesql::Coercer coercer(type_factory(), &language_options,
                             /*catalog=*/this);
  googlesql::InputArgumentType input_type(from_type);
  googlesql::SignatureMatchResult result;
  auto evaluator = googlesql::ExtendedCompositeCastEvaluator::Invalid();
  return coercer.CoercesTo(input_type, to_type,
                           /*is_explicit=*/true, &result, &evaluator);
}

absl::Status EngineSystemCatalog::AddType(
    const PostgresTypeMapping* type,
    const googlesql::LanguageOptions& language_options) {
  googlesql::ProductMode product_mode = language_options.product_mode();
  GOOGLESQL_VLOG(4) << "PostgresCatalog::AddType, type = "
          << type->TypeName(product_mode);
  engine_types_.insert({std::string(type->raw_type_name()), type});

  const googlesql::Type* gsql_type = type->mapped_type();
  // Special case (2 to 1): `PgVarcharMapping` and `PgTextMapping` both map to
  // `googlesql::types::StringType`. Use `PgTextMapping` as the
  // default type for reverse mapping in the catalog.
  // Same for `PgVarcharArrayMapping` and `PgTextArrayMapping`.
  if (gsql_type && type != types::PgVarcharMapping() &&
      type != types::PgVarcharArrayMapping()) {
    GOOGLESQL_VLOG(4) << "PostgresCatalog::AddType, type.mapped_type = "
            << gsql_type->TypeName(product_mode);
    bool unique = engine_types_reverse_map_
                      .insert({gsql_type, std::string(type->raw_type_name())})
                      .second;

    GOOGLESQL_RET_CHECK(unique) << absl::Substitute(
        "Multiple PostgresTypeMapping will be mapped to the builtin type: "
        "$0, the last PostgresTypeMapping is: $1",
        gsql_type->TypeName(product_mode), type->TypeName(product_mode));
  }

  return absl::OkStatus();
}

bool EngineSystemCatalog::SignatureMatches(
    const std::vector<googlesql::InputArgumentType>& input_arguments,
    const googlesql::FunctionSignature& signature,
    std::unique_ptr<googlesql::FunctionSignature>* result_signature,
    const googlesql::LanguageOptions& language_options) {
  // Run the GoogleSQL Function Signature Matcher to determine if the input
  // arguments exactly match the signature
  googlesql::Coercer coercer(type_factory(), &language_options,
                             /*catalog=*/this);
  const std::vector<const googlesql::ASTNode*> arg_ast_nodes = {};
  googlesql::SignatureMatchResult signature_match_result;
  absl::StatusOr<bool> function_signature_matches_or =
      googlesql::FunctionSignatureMatchesWithStatus(
          language_options, coercer, arg_ast_nodes, input_arguments, signature,
          /*allow_argument_coercion=*/false, type_factory(),
          /*resolve_lambda_callback=*/nullptr, result_signature,
          &signature_match_result,
          /*arg_index_mapping=*/nullptr, /*arg_overrides=*/nullptr);
  ABSL_DCHECK_OK(function_signature_matches_or.status());

  return function_signature_matches_or.value_or(false) &&
         signature_match_result.non_matched_arguments() == 0 &&
         signature_match_result.non_literals_coerced() == 0 &&
         signature_match_result.literals_coerced() == 0;
}

absl::StatusOr<googlesql::FunctionArgumentType>
EngineSystemCatalog::BuildGsqlFunctionArgumentType(
    Oid type_oid, googlesql::FunctionEnums::ArgumentCardinality cardinality) {
  if (type_oid == ANYOID || type_oid == ANYELEMENTOID ||
      // Technically this is more permissive than we should be
      // (ARG_KIND_EXPR_ANY_1 would actually accept an array type too), but the
      // specific function signatures we register handle this because they refer
      // to specific, non-pseudo types.
      type_oid == ANYNONARRAYOID) {
    return googlesql::FunctionArgumentType(googlesql::ARG_KIND_EXPR_ANY_1,
                                           cardinality);
  } else if (type_oid == ANYARRAYOID || type_oid == ANYCOMPATIBLEARRAYOID) {
    return googlesql::FunctionArgumentType(googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
                                           cardinality);
  } else {
    // Get the PostgresTypeMapping.
    const PostgresTypeMapping* type = GetType(type_oid);

    if (type == nullptr) {
      return absl::NotFoundError(
          absl::StrCat("No PostgresTypeMapping with oid: ", type_oid));
    }

    // If there is a mapped builtin type, use it.
    // Otherwise use the PostgreSQL type.
    if (type->mapped_type()) {
      return googlesql::FunctionArgumentType(type->mapped_type(), cardinality);
    } else {
      return googlesql::FunctionArgumentType(type, cardinality);
    }
  }
}

absl::StatusOr<googlesql::FunctionSignature>
EngineSystemCatalog::BuildGsqlFunctionSignature(
    const absl::Span<const Oid>& postgres_input_types, Oid postgres_output_type,
    Oid postgres_variadic_type, bool postgres_retset) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      googlesql::FunctionArgumentType pg_return_type,
      BuildGsqlFunctionArgumentType(postgres_output_type,
                                    googlesql::FunctionEnums::REQUIRED));
  std::unique_ptr<googlesql::FunctionArgumentType> return_type_ptr;
  if (postgres_retset) {
    const googlesql::Type* array_type;
    GOOGLESQL_RETURN_IF_ERROR(
        type_factory()->MakeArrayType(pg_return_type.type(), &array_type));
    googlesql::FunctionArgumentType array_argument_type =
        googlesql::FunctionArgumentType(array_type,
                                        googlesql::FunctionEnums::REQUIRED);
    return_type_ptr =
        std::make_unique<googlesql::FunctionArgumentType>(array_argument_type);
  } else {
    return_type_ptr =
        std::make_unique<googlesql::FunctionArgumentType>(pg_return_type);
  }

  googlesql::FunctionArgumentTypeList arguments;
  for (int i = 0; i < postgres_input_types.size(); i++) {
    Oid input_type_oid = postgres_input_types[i];
    auto cardinality = input_type_oid == postgres_variadic_type
                           ? googlesql::FunctionEnums::REPEATED
                           : googlesql::FunctionEnums::REQUIRED;
    GOOGLESQL_ASSIGN_OR_RETURN(
        googlesql::FunctionArgumentType input_type,
        BuildGsqlFunctionArgumentType(input_type_oid, cardinality));
    arguments.push_back(input_type);
  }

  return googlesql::FunctionSignature(*return_type_ptr, arguments,
                                      /*context_ptr=*/nullptr);
}

absl::StatusOr<std::vector<googlesql::InputArgumentType>>
EngineSystemCatalog::BuildGsqlInputTypeList(
    const oidvector& postgres_input_types) {
  std::vector<googlesql::InputArgumentType> input_arguments;
  for (int i = 0; i < postgres_input_types.vl_len_; i++) {
    // Get the PostgresTypeMapping.
    const PostgresTypeMapping* type = GetType(postgres_input_types.values[i]);

    if (type == nullptr) {
      return absl::NotFoundError(absl::StrCat(
          "No PostgresTypeMapping with oid: ", postgres_input_types.values[i]));
    }

    // If there is a mapped builtin type, use it.
    // Otherwise use the PostgreSQL type.
    if (type->mapped_type()) {
      input_arguments.emplace_back(type->mapped_type());
    } else {
      input_arguments.emplace_back(type);
    }
  }
  return input_arguments;
}

absl::StatusOr<Oid> EngineSystemCatalog::FindMatchingPgProcOid(
    absl::Span<const PgProcData* const> procs,
    const std::vector<googlesql::InputArgumentType>& input_argument_types,
    const googlesql::Type* return_type,
    const googlesql::LanguageOptions& language_options) {
  bool srf_ret_type_mismatch = false;
  for (const PgProcData* proc_proto : procs) {
    // Convert the proc from a PostgreSQL signature into a GoogleSQL signature
    absl::StatusOr<const googlesql::FunctionSignature> postgres_signature =
        BuildGsqlFunctionSignature(
            proc_proto->proargtypes(), proc_proto->prorettype(),
            proc_proto->provariadic(), proc_proto->proretset());

    // If there is a problem transforming the postgres signature, potentially
    // due to unsupported types, just skip the proc.
    if (!postgres_signature.ok()) {
      srf_ret_type_mismatch = proc_proto->proretset();
      continue;
    }

    // If the return type doesn't match, skip it, but permit pseudo ANYARRAY
    // type to match any particular array type.
    if (!TypesMatch(postgres_signature->result_type().type(), return_type)) {
      if ((proc_proto->prorettype() != ANYARRAYOID &&
           proc_proto->prorettype() != ANYCOMPATIBLEARRAYOID) ||
          !return_type->IsArray()) {
        continue;
      }
    }

    // If we find a matching PostgreSQL proc oid, return it.
    std::unique_ptr<googlesql::FunctionSignature> result_signature;
    if (SignatureMatches(input_argument_types, *postgres_signature,
                         &result_signature, language_options)) {
      return proc_proto->oid();
    }
  }
  return absl::UnimplementedError(
      "No Postgres proc oid found for the provided argument types");
}

void EngineSystemCatalog::AddFunctionToReverseMappings(
    const std::string& proc_name, Oid proc_oid) {
  // Always add the function to the reverse non-operator map.
  // Some functions that are variadic can use the operator in reverse
  // transformation when there are exactly 2 arguments but should use the
  // function in reverse transformation when there are 1 or 3+ arguments.
  auto reverse_function_it =
      engine_function_non_operators_reverse_map_.find(proc_name);
  if (reverse_function_it == engine_function_non_operators_reverse_map_.end()) {
    engine_function_non_operators_reverse_map_.insert({proc_name, {proc_oid}});
  } else {
    reverse_function_it->second.push_back(proc_oid);
  }
  // If the function is also an operator, add it to the reverse operator map.
  absl::StatusOr<absl::Span<const Oid>> operator_oids =
      PgBootstrapCatalog::Default()->GetOperatorOidsByOprcode(proc_oid);
  if (operator_oids.ok()) {
    // The function is also an operator. Add it to the reverse operator map.
    auto reverse_operator_it =
        engine_function_operators_reverse_map_.find(proc_name);
    if (reverse_operator_it == engine_function_operators_reverse_map_.end()) {
      engine_function_operators_reverse_map_.insert({proc_name, {proc_oid}});
    } else {
      reverse_operator_it->second.push_back(proc_oid);
    }
  }
}

absl::Status EngineSystemCatalog::AddTVF(Oid proc_oid,
                                         const std::string& engine_tvf_name) {
  GOOGLESQL_ASSIGN_OR_RETURN(const FormData_pg_proc* pg_proc,
                   PgBootstrapCatalog::Default()->GetProc(proc_oid));
  GOOGLESQL_RET_CHECK_EQ(pg_proc->proretset, true);
  GOOGLESQL_ASSIGN_OR_RETURN(
      const googlesql::TableValuedFunction* tvf,
      builtin_function_catalog_->GetTableValuedFunction(engine_tvf_name));
  proc_oid_to_tvf_[proc_oid] = tvf;
  tvf_to_proc_oid_[tvf] = proc_oid;
  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::AddProcedure(
    Oid proc_oid, const std::string& engine_procedure_name) {
  GOOGLESQL_ASSIGN_OR_RETURN(const FormData_pg_proc* pg_proc,
                   PgBootstrapCatalog::Default()->GetProc(proc_oid));
  GOOGLESQL_RET_CHECK_EQ(pg_proc->prokind, 'p');
  GOOGLESQL_RET_CHECK_EQ(pg_proc->prorettype, VOIDOID);
  GOOGLESQL_ASSIGN_OR_RETURN(
      const googlesql::Procedure* procedure,
      builtin_function_catalog_->GetProcedure(engine_procedure_name));
  proc_oid_to_procedure_[proc_oid] = procedure;
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<PostgresExtendedFunctionSignature>>
EngineSystemCatalog::BuildVariadicPostgresExtendedFunctionSignature(
    const std::string& mapped_function_name,
    const googlesql::FunctionSignature& signature, Oid proc_oid,
    const std::vector<std::string>& query_features_names) {
  // Get the original builtin function.
  const googlesql::Function* mapped_builtin_function;
  GOOGLESQL_ASSIGN_OR_RETURN(
      mapped_builtin_function,
      builtin_function_catalog_->GetFunction(mapped_function_name));
  GOOGLESQL_RET_CHECK_NE(mapped_builtin_function, nullptr);

  return std::make_unique<PostgresExtendedFunctionSignature>(
      signature,
      std::make_unique<googlesql::Function>(
          mapped_builtin_function->FunctionNamePath(),
          mapped_builtin_function->GetGroup(), mapped_builtin_function->mode(),
          std::vector<googlesql::FunctionSignature>{signature},
          mapped_builtin_function->function_options()),
      proc_oid, query_features_names);
}

absl::Status EngineSystemCatalog::AddFunction(
    const PostgresFunctionArguments& function_arguments,
    const googlesql::LanguageOptions& language_options) {
  // The function should not already be in the catalog.
  std::pair<std::string, std::string> function_key =
      std::make_pair(function_arguments.postgres_namespace(),
                     function_arguments.postgres_function_name());
  if (engine_functions_.find(function_key) != engine_functions_.end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Function ", function_arguments.postgres_namespace(), ".",
                     function_arguments.postgres_function_name(),
                     " was already added to the catalog."));
  }

  const std::vector<std::string>& function_query_features_names =
      function_arguments.query_features_names();

  // Get the list of procs that match the PostgreSQL proc name and namespace
  // name.
  GOOGLESQL_ASSIGN_OR_RETURN(Oid namespace_oid,
                   PgBootstrapCatalog::Default()->GetNamespaceOid(
                       function_arguments.postgres_namespace()));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Span<const FormData_pg_proc* const> proc_data,
                   PgBootstrapCatalog::Default()->GetProcsByName(
                       function_arguments.postgres_function_name()));
  std::vector<const PgProcData*> procs;
  for (const FormData_pg_proc* proc : proc_data) {
    if (proc->pronamespace == namespace_oid) {
      GOOGLESQL_ASSIGN_OR_RETURN(const PgProcData* proc_proto,
                       PgBootstrapCatalog::Default()->GetProcProto(proc->oid));
      procs.push_back(proc_proto);
    }
  }

  std::vector<std::unique_ptr<PostgresExtendedFunctionSignature>>
      function_signatures;
  for (const PostgresFunctionSignatureArguments& signature_arguments :
       function_arguments.signature_arguments()) {
    // Get the mapped builtin function name and function.
    const std::string& mapped_function_name =
        signature_arguments.explicit_mapped_function_name().empty()
            ? function_arguments.mapped_function_name()
            : signature_arguments.explicit_mapped_function_name();
    const std::vector<std::string>& signature_query_features_names =
        signature_arguments.query_features_names();

    if (signature_arguments.postgres_proc_oid() != InvalidOid) {
      // The proc oid is provided and validation should not be performed.
      // Use the provided proc oid and signature as-is.
      GOOGLESQL_RET_CHECK(signature_arguments.has_mapped_function());
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::unique_ptr<PostgresExtendedFunctionSignature> signature,
          BuildVariadicPostgresExtendedFunctionSignature(
              mapped_function_name, signature_arguments.signature(),
              signature_arguments.postgres_proc_oid(),
              signature_query_features_names));
      function_signatures.push_back(std::move(signature));
      AddFunctionToReverseMappings(mapped_function_name,
                                   signature_arguments.postgres_proc_oid());
      continue;
    }

    googlesql::FunctionSignature engine_system_catalog_signature =
        signature_arguments.signature();
    if (engine_system_catalog_signature.NumRepeatedArguments() != 0 ||
        engine_system_catalog_signature.NumOptionalArguments() != 0) {
      return absl::UnimplementedError(
          "Variadic Postgres functions must have the proc oid explicitly "
          "provided");
    }

    // Turn the EngineSystemCatalog signature into a list of input argument
    // types which will be used to signature match against the PostgreSQL
    // procs and the mapped builtin functions.
    std::vector<googlesql::InputArgumentType> input_arguments;
    for (const googlesql::FunctionArgumentType& arg :
         engine_system_catalog_signature.arguments()) {
      input_arguments.emplace_back(arg.type());
    }

    // Get and verify the mapped PostgreSQL proc oid.
    GOOGLESQL_ASSIGN_OR_RETURN(
        Oid oid,
        FindMatchingPgProcOid(
            procs, input_arguments,
            engine_system_catalog_signature.result_type().type(),
            language_options),
        _ << absl::StrCat(
            "Function ", function_arguments.postgres_function_name(),
            " with signature: ",
            engine_system_catalog_signature.DebugString(/*function_name=*/"",
                                                        /*verbose=*/true)));

    if (!signature_arguments.has_mapped_function()) {
      function_signatures.push_back(
          std::make_unique<PostgresExtendedFunctionSignature>(
              engine_system_catalog_signature, /*mapped_function=*/nullptr, oid,
              signature_query_features_names));
    } else {
      // Get and verify a copy of the mapped builtin function and signature for
      // specific signature.
      GOOGLESQL_RET_CHECK(!mapped_function_name.empty());
      // Get the original builtin function.
      const googlesql::Function* mapped_builtin_function;
      GOOGLESQL_ASSIGN_OR_RETURN(
          mapped_builtin_function,
          builtin_function_catalog_->GetFunction(mapped_function_name));
      GOOGLESQL_RET_CHECK_NE(mapped_builtin_function, nullptr)
          << "Original function " << mapped_function_name
          << " not found in builtin function catalog";

      // Check that the original builtin function has a compatible signature and
      // create a copy of the function with just this signature.
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::unique_ptr<googlesql::Function> mapped_function,
          BuildMappedFunction(engine_system_catalog_signature, input_arguments,
                              mapped_builtin_function, language_options));

      GOOGLESQL_RET_CHECK_EQ(mapped_function->NumSignatures(), 1);
      const googlesql::FunctionSignature* mapped_signature =
          mapped_function->GetSignature(0);

      // Copy over the signature options from the builtin-function signature.
      engine_system_catalog_signature = googlesql::FunctionSignature(
          engine_system_catalog_signature.result_type(),
          engine_system_catalog_signature.arguments(),
          mapped_signature->context_id(),
          mapped_signature->options());

      // Construct the PostgresExtendedFunctionSignature.
      function_signatures.push_back(
          std::make_unique<PostgresExtendedFunctionSignature>(
              engine_system_catalog_signature, std::move(mapped_function), oid,
              signature_query_features_names));
      AddFunctionToReverseMappings(mapped_function_name, oid);
    }
  }

  std::unique_ptr<PostgresExtendedFunction> function =
      std::make_unique<PostgresExtendedFunction>(
          function_arguments.postgres_function_name(),
          function_arguments.mode(), std::move(function_signatures),
          function_query_features_names);

  engine_functions_.insert({function_key, std::move(function)});

  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::AddExprFunction(
    const PostgresExprIdentifier& expr_id,
    const std::string& builtin_function_name) {
  // Look up the builtin function by name.
  const googlesql::Function* builtin_function;
  GOOGLESQL_ASSIGN_OR_RETURN(builtin_function, builtin_function_catalog_->GetFunction(
                                         builtin_function_name));
  GOOGLESQL_RET_CHECK_NE(builtin_function, nullptr);

  // Update the forward map.
  pg_expr_to_builtin_function_.insert({expr_id, builtin_function});

  // Update the reverse map.
  engine_function_expr_reverse_map_.insert({builtin_function_name, expr_id});
  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::AddPgNumericCastFunction(
    const std::string& builtin_function_name) {
  GOOGLESQL_RET_CHECK(!engine_cast_functions_.contains(builtin_function_name))
      << "Attempting to insert duplicate cast function.";
  engine_cast_functions_.insert(builtin_function_name);
  return absl::OkStatus();
}

absl::Status EngineSystemCatalog::AddCastOverrideFunction(
    const googlesql::Type* source_type, const googlesql::Type* target_type,
    const std::string& builtin_function_name,
    const googlesql::LanguageOptions& language_options) {
  std::pair<const googlesql::Type*, const googlesql::Type*> cast_pair(
      source_type, target_type);

  // Look up the function and validate the input/output.
  const googlesql::Function* builtin_function;
  GOOGLESQL_ASSIGN_OR_RETURN(builtin_function, builtin_function_catalog_->GetFunction(
                                         builtin_function_name));
  std::vector<googlesql::InputArgumentType> input_argument_types;
  input_argument_types.push_back(googlesql::InputArgumentType(source_type));
  for (const googlesql::FunctionSignature& signature :
       builtin_function->signatures()) {
    // Run the function signature matcher to check if the builtin signature
    // matches the input argument types.
    std::unique_ptr<googlesql::FunctionSignature> result_signature;
    if (SignatureMatches(input_argument_types, signature, &result_signature,
                         language_options) &&
        result_signature->result_type().type()->Equals(target_type)) {
      // We store the result signature instead of the mapped signature
      // because the the Function Signature Matcher fills in the actual
      // types if the original signature had ARG_KIND_EXPR_ANY_1 input or output
      // types.
      GOOGLESQL_RET_CHECK(!pg_cast_to_builtin_function_.contains(cast_pair))
          << "Attempting to insert duplicate cast function.";
      pg_cast_to_builtin_function_.insert(
          {cast_pair,
           FunctionAndSignature(builtin_function, *result_signature)});

      // Same function name can have multiple signatures.
      // For example, PG_CAST_TO_STRING has signature for INTERVAL and
      // PG.NUMERIC.
      if (!engine_cast_functions_.contains(builtin_function_name)) {
        engine_cast_functions_.insert(builtin_function_name);
      }

      return absl::OkStatus();
    }
  }
  return absl::NotFoundError(absl::StrFormat(
      "Unable to find cast override function named \"%s\" for casting <%s> to "
      "<%s>",
      builtin_function_name,
      source_type->TypeName(language_options.product_mode()),
      target_type->TypeName(language_options.product_mode())));
}

absl::StatusOr<const googlesql::Function*>
EngineSystemCatalog::GetBuiltinFunction(const std::string& name) const {
  return builtin_function_catalog_->GetFunction(name);
}

bool EngineSystemCatalog::IsBuiltinSqlRewriteFunction(
    const std::string& function_name,
    const googlesql::LanguageOptions& language_options,
    googlesql::TypeFactory* type_factory) {
  googlesql::BuiltinFunctionOptions function_options(language_options);
  absl::flat_hash_map<std::string, std::unique_ptr<googlesql::Function>>
      spanner_function_map;
  absl::flat_hash_map<std::string, const googlesql::Type*> types;
  googlesql::GetBuiltinFunctionsAndTypes(function_options, *type_factory,
                               spanner_function_map, types);
  if (spanner_function_map.find(function_name) ==
      spanner_function_map.end()) {
    return false;
  }
  const googlesql::Function* builtin_function =
      spanner_function_map.at(function_name).get();
  for (const auto& signature : builtin_function->signatures()) {
    if (!signature.options().rewrite_options() ||
        signature.options().rewrite_options()->sql().empty()) {
      return false;
    }
  }
  return true;
}

}  // namespace postgres_translator
