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

#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor.h"
#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

const PgBootstrapCatalog* GetPgBootstrapCatalog() {
  return PgBootstrapCatalog::Default();
}

absl::StatusOr<PgTypeData> GetPgTypeDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view type_name) {
  ZETASQL_ASSIGN_OR_RETURN(auto types, catalog->GetTypesByName(type_name));
  if (types.empty()) {
    return absl::NotFoundError(
        absl::StrCat("Type not found: ", type_name));
  }
  auto type = types.at(0);
  PgTypeData type_data;
  type_data.set_oid(type->oid);
  type_data.set_typname(type->typname.data);
  type_data.set_typnamespace(type->typnamespace);
  type_data.set_typlen(type->typlen);
  type_data.set_typbyval(type->typbyval);
  type_data.set_typtype(absl::StrFormat("%c", type->typtype));
  type_data.set_typcategory(absl::StrFormat("%c", type->typcategory));
  type_data.set_typispreferred(type->typispreferred);
  type_data.set_typisdefined(type->typisdefined);
  type_data.set_typdelim(absl::StrFormat("%c", type->typdelim));
  type_data.set_typelem(type->typelem);
  type_data.set_typarray(type->typarray);
  return type_data;
}

absl::StatusOr<PgTypeData> GetPgTypeDataFromBootstrap(
    const PgBootstrapCatalog* catalog, int64_t type_oid) {
  ZETASQL_ASSIGN_OR_RETURN(auto type, catalog->GetType(type_oid));
  PgTypeData type_data;
  type_data.set_oid(type->oid);
  type_data.set_typname(type->typname.data);
  type_data.set_typnamespace(type->typnamespace);
  type_data.set_typlen(type->typlen);
  type_data.set_typbyval(type->typbyval);
  type_data.set_typtype(absl::StrFormat("%c", type->typtype));
  type_data.set_typcategory(absl::StrFormat("%c", type->typcategory));
  type_data.set_typispreferred(type->typispreferred);
  type_data.set_typisdefined(type->typisdefined);
  type_data.set_typdelim(absl::StrFormat("%c", type->typdelim));
  type_data.set_typelem(type->typelem);
  type_data.set_typarray(type->typarray);
  return type_data;
}

absl::StatusOr<PgCollationData> GetPgCollationDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view collation_name) {
  ZETASQL_ASSIGN_OR_RETURN(auto collation, catalog->GetCollationByName(collation_name));
  PgCollationData collation_data;
  collation_data.set_oid(collation->oid);
  collation_data.set_collname(collation->collname.data);
  collation_data.set_collprovider(
      absl::StrFormat("%c", collation->collprovider));
  collation_data.set_collisdeterministic(collation->collisdeterministic);
  collation_data.set_collencoding(collation->collencoding);
  return collation_data;
}

absl::StatusOr<PgNamespaceData> GetPgNamespaceDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view namespace_name) {
  ZETASQL_ASSIGN_OR_RETURN(auto nsp_oid, catalog->GetNamespaceOid(namespace_name));
  PgNamespaceData namespace_data;
  namespace_data.set_oid(nsp_oid);
  namespace_data.set_nspname(namespace_name);
  return namespace_data;
}

absl::StatusOr<PgProcData> GetPgProcDataFromBootstrap(
    const PgBootstrapCatalog* catalog, int64_t proc_oid) {
  ZETASQL_ASSIGN_OR_RETURN(auto proc, catalog->GetProcProto(proc_oid));
  PgProcData proc_data;
  proc_data.CopyFrom(*proc);
  proc_data.clear_proowner();
  proc_data.clear_prolang();
  proc_data.clear_procost();
  proc_data.clear_prosupport();
  proc_data.clear_prosecdef();
  proc_data.clear_prosrc();
  return proc_data;
}

absl::StatusOr<std::vector<PgProcData>> GetPgProcDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view proc_name) {
  ZETASQL_ASSIGN_OR_RETURN(auto procs, catalog->GetProcsByName(proc_name));
  std::vector<PgProcData> procs_data;
  for (const auto& proc : procs) {
    ZETASQL_ASSIGN_OR_RETURN(auto proc_data,
                     GetPgProcDataFromBootstrap(catalog, proc->oid));
    procs_data.push_back(proc_data);
  }
  return procs_data;
}

}  // namespace postgres_translator
