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

#include "third_party/spanner_pg/catalog/catalog_adapter.h"

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/function.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/engine_user_catalog.h"
#include "third_party/spanner_pg/catalog/table_name.h"
#include "third_party/spanner_pg/errors/error_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

absl::StatusOr<std::unique_ptr<CatalogAdapter>> CatalogAdapter::Create(
    std::unique_ptr<EngineUserCatalog> engine_user_catalog,
    EngineSystemCatalog* engine_system_catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    absl::flat_hash_map<int, int> token_locations) {
  if (engine_user_catalog == nullptr) {
    return spangres::internal_error::EmptyEngineCatalogForCatalogAdapter(
        "user");
  }

  if (engine_system_catalog == nullptr) {
    return spangres::internal_error::EmptyEngineCatalogForCatalogAdapter(
        "system");
  }

  // Use WrapUnique() to return a unique pointer from this factory method, as
  // CatalogAdapter has a private constructor.
  return absl::WrapUnique(
      new CatalogAdapter(std::move(engine_user_catalog), engine_system_catalog,
                         analyzer_options, std::move(token_locations)));
}

CatalogAdapter::CatalogAdapter(
    std::unique_ptr<EngineUserCatalog> engine_user_catalog,
    EngineSystemCatalog* engine_system_catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    absl::flat_hash_map<int, int> token_locations)
    : engine_user_catalog_(std::move(engine_user_catalog)),
      engine_system_catalog_(engine_system_catalog),
      analyzer_options_(analyzer_options),
      token_locations_(std::move(token_locations)) {}

// Allocates a new column id using a counter. This method is the same as the
// ZetaSQL version, except that it throws an InternalError in error cases.
absl::StatusOr<int> CatalogAdapter::AllocateColumnId() {
  if (max_column_id_ == std::numeric_limits<int32_t>::max()) {
    return absl::InternalError(
        absl::StrCat("Cannot allocate new column id. Max id ", max_column_id_,
                     " has been reached."));
  }

  ++max_column_id_;
  return max_column_id_;
}

absl::StatusOr<TableName> CatalogAdapter::GetTableNameFromOid(Oid oid) const {
  if (oid <= kOidCounterEnd || oid >= kOidCounterStart) {
    return absl::InternalError(absl::StrFormat(
        "Valid table oids are from %u to %u, inclusive; got %u instead",
        kOidCounterEnd + 1, kOidCounterStart - 1, oid));
  }
  auto iter = oid_to_table_map_.find(oid);
  if (iter == oid_to_table_map_.end())
    return absl::InternalError(absl::StrCat(
        "No table with oid ", oid, " is tracked by the catalog adapter"));
  std::pair<Oid, std::string> table_identifier = iter->second;
  ZETASQL_ASSIGN_OR_RETURN(std::string namespace_name,
                   GetNamespaceNameFromOid(table_identifier.first));
  return TableName({namespace_name, table_identifier.second});
}

absl::StatusOr<Oid> CatalogAdapter::GetOrGenerateOidFromTableName(
    const TableName& table_name) {
  // Find the namespace for this table, assuming "public" if none was supplied.
  Oid schema_oid;
  const std::string* namespace_name_ptr = table_name.NamespaceName();
  if (namespace_name_ptr == nullptr) {
    // NOTE: Currently, we map a name to (synthesized) OID first,
    // then look it up in the catalog. To correctly handle search paths, we
    // would instead have to find the table first (with multiple search prefixes
    // if necessary), and then use the found table and add the name mapping
    // based on the table. In that case, if we get an unqualified name in this
    // method we really are dealing with a table in "public". Without proper
    // search path behavior, we behave as if "public" is the sole search path,
    // and pretend all unqualified names are within that namespace.
    ZETASQL_ASSIGN_OR_RETURN(schema_oid, PgBootstrapCatalog::Default()->GetNamespaceOid(
                                     EngineUserCatalog::kPublicSchema));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(schema_oid,
                     GetOrGenerateOidFromNamespaceName(*namespace_name_ptr));
  }
  return GetOrGenerateOidFromNamespaceOidAndRelationName(
      schema_oid, table_name.UnqualifiedName());
}

absl::StatusOr<Oid>
CatalogAdapter::GetOrGenerateOidFromNamespaceOidAndRelationName(
    Oid namespace_oid, const std::string& relation_name) {
  auto iter = table_to_oid_map_.find({namespace_oid, relation_name});
  if (iter != table_to_oid_map_.end()) {
    return iter->second;
  } else {
    ZETASQL_ASSIGN_OR_RETURN(Oid new_table_oid, GenerateOid());
    ZETASQL_RETURN_IF_ERROR(
        AddOidAndTableNameToMaps(namespace_oid, new_table_oid, relation_name));
    return new_table_oid;
  }
}

absl::StatusOr<Oid> CatalogAdapter::GetOrGenerateOidFromNamespaceName(
    const std::string& namespace_name) {
  absl::StatusOr<Oid> oid_or = GetOidFromNamespaceName(namespace_name);
  if (oid_or.ok()) {
    // Previously seen or bootstrap oid.
    return *oid_or;
  } else if (absl::IsNotFound(oid_or.status())) {
    // Previously unknown: generate a new oid.
    ZETASQL_ASSIGN_OR_RETURN(Oid new_oid, GenerateOid());
    ZETASQL_RETURN_IF_ERROR(AddOidAndNamespaceNameToMaps(new_oid, namespace_name));
    return new_oid;
  } else {
    // Something went wrong.
    return oid_or.status();
  }
}

absl::StatusOr<Oid> CatalogAdapter::GetOidFromNamespaceName(
    const std::string& namespace_name) {
  // Is this a bootstrap namespace?
  absl::StatusOr<Oid> oid_status =
      PgBootstrapCatalog::Default()->GetNamespaceOid(namespace_name);
  if (oid_status.ok()) {
    return *oid_status;
  }

  // Did we previously generate an Oid for this?
  auto iter = namespace_to_oid_map_.find(namespace_name);
  if (iter != namespace_to_oid_map_.end()) {
    return iter->second;
  }

  return absl::NotFoundError(
      absl::StrCat("No namespace with name ", namespace_name,
                   " is tracked by the catalog adapter"));
}

absl::StatusOr<Oid> CatalogAdapter::GenerateAndStoreUDFProcOid(
    FormData_pg_proc* pg_proc, const zetasql::Function* udf) {
  ZETASQL_RET_CHECK_NE(pg_proc, nullptr);
  ZETASQL_RET_CHECK_NE(udf, nullptr);
  // We're about to generate a new oid. Make sure there isn't one already.
  if (pg_proc->oid != InvalidOid) {
    return absl::InternalError("pg_proc already has an oid assigned");
  }
  ZETASQL_ASSIGN_OR_RETURN(pg_proc->oid, GenerateOid());
  oid_to_udf_proc_map_[pg_proc->oid] = pg_proc;
  oid_to_udf_map_[pg_proc->oid] = udf;
  return pg_proc->oid;
}

absl::StatusOr<Oid> CatalogAdapter::GenerateAndStoreTVFProcOid(
    FormData_pg_proc* pg_proc, const zetasql::TableValuedFunction* tvf) {
  ZETASQL_RET_CHECK_NE(pg_proc, nullptr);
  ZETASQL_RET_CHECK_NE(tvf, nullptr);
  // We're about to generate a new oid. Make sure there isn't one already.
  if (pg_proc->oid != InvalidOid) {
    return absl::InternalError("pg_proc already has an oid assigned");
  }
  ZETASQL_ASSIGN_OR_RETURN(pg_proc->oid, GenerateOid());
  oid_to_udf_proc_map_[pg_proc->oid] = pg_proc;
  oid_to_tvf_map_[pg_proc->oid] = tvf;
  return pg_proc->oid;
}

absl::StatusOr<const FormData_pg_proc*> CatalogAdapter::GetUDFProcFromOid(
    Oid oid) const {
  auto iter = oid_to_udf_proc_map_.find(oid);
  if (iter == oid_to_udf_proc_map_.end()) {
    return absl::NotFoundError(absl::StrCat(
        "No UDF proc with oid ", oid, " is tracked by the catalog adapter"));
  }
  return iter->second;
}

absl::StatusOr<const zetasql::Function*> CatalogAdapter::GetUDFFromOid(
    Oid oid) const {
  auto iter = oid_to_udf_map_.find(oid);
  if (iter != oid_to_udf_map_.end()) {
    return iter->second;
  }

  return absl::NotFoundError(
      absl::StrCat("UDF with oid ", oid, " is not supported"));
}

absl::StatusOr<const zetasql::TableValuedFunction*>
CatalogAdapter::GetTVFFromOid(Oid oid) const {
  // TVFs can be user defined and assigned a temporary Oid in the CatalogAdapter
  // or they can be builtin and found in the EngineSystemCatalog.
  auto iter = oid_to_tvf_map_.find(oid);
  if (iter != oid_to_tvf_map_.end()) {
    return iter->second;
  }

  auto builtin_tvf = engine_system_catalog_->GetTableValuedFunction(oid);
  if (builtin_tvf != nullptr) {
    return builtin_tvf;
  }

  return absl::NotFoundError(
      absl::StrCat("TVF with oid ", oid, " is not supported"));
}

absl::Status CatalogAdapter::AddOidAndTableNameToMaps(
    const Oid schema_oid, const Oid table_oid,
    const std::string& relation_name) {
  // Check for duplicates.
  auto table_iter = oid_to_table_map_.find(table_oid);
  if (table_iter != oid_to_table_map_.end()) {
    return absl::InternalError(
        absl::StrCat("Table with oid ", table_oid,
                     " already exists in the catalog adapter"));
  }
  auto oid_iter = table_to_oid_map_.find({schema_oid, relation_name});
  if (oid_iter != table_to_oid_map_.end()) {
    return absl::InternalError(absl::StrCat(
        "Table with name ", relation_name, "in namespace with Oid ", schema_oid,
        " already exists in the catalog adapter"));
  }

  oid_to_table_map_.insert({table_oid, {schema_oid, relation_name}});
  table_to_oid_map_.insert({{schema_oid, relation_name}, table_oid});
  return absl::OkStatus();
}

absl::Status CatalogAdapter::AddOidAndNamespaceNameToMaps(
    const Oid namespace_oid, const std::string& namespace_name) {
  auto oid_iter = namespace_to_oid_map_.find(namespace_name);
  if (oid_iter != namespace_to_oid_map_.end()) {
    return absl::InternalError(
        absl::StrCat("Namespace with name ", namespace_name,
                     " already exists in the catalog adapter"));
  }
  auto name_iter = oid_to_namespace_map_.find(namespace_oid);
  if (name_iter != oid_to_namespace_map_.end()) {
    return absl::InternalError(
        absl::StrCat("Namespace with Oid ", namespace_oid,
                     " already exists in the catalog adapter"));
  }
  oid_to_namespace_map_.insert({namespace_oid, namespace_name});
  namespace_to_oid_map_.insert({namespace_name, namespace_oid});
  return absl::OkStatus();
}

absl::StatusOr<std::string> CatalogAdapter::GetNamespaceNameFromOid(
    Oid oid) const {
  // Bootstrap namespace?
  absl::StatusOr<const char*> oid_status =
      PgBootstrapCatalog::Default()->GetNamespaceName(oid);
  if (oid_status.ok()) {
    return std::string(*oid_status);
  }
  // Did we previously generate an oid for this?
  auto iter = oid_to_namespace_map_.find(oid);
  if (iter != oid_to_namespace_map_.end()) {
    return iter->second;
  }
  return absl::InternalError(absl::StrCat(
      "No namespace with oid ", oid, " is tracked by the catalog adapter"));
}

absl::StatusOr<Oid> CatalogAdapter::GenerateOid() {
  Oid new_oid = --oid_counter_;
  if (oid_counter_ <= kOidCounterEnd) {
    return absl::InternalError(
        absl::StrFormat("Oids for tables have run out of range. "
                        "Normal range is from %u to %u",
                        kOidCounterEnd + 1, kOidCounterStart - 1));
  }
  return new_oid;
}

// EngineUserCatalog wrapper provides schema_name mapping and other PostgreSQL
// features.
EngineUserCatalog* CatalogAdapter::GetEngineUserCatalog() {
  return engine_user_catalog_.get();
}

EngineSystemCatalog* CatalogAdapter::GetEngineSystemCatalog() {
  return engine_system_catalog_;
}

}  // namespace postgres_translator
