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

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <type_traits>

#include "zetasql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/pg_catalog_util.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

absl::StatusOr<const zetasql::Type*> ForwardTransformer::BuildGsqlType(
    const Oid pg_type_oid) {
  if (pg_type_oid == UNKNOWNOID) {
    return absl::InvalidArgumentError(
        "Unable to resolve argument type. "
        "Please consider adding an explicit cast");
  }
  EngineSystemCatalog* engine_system_catalog =
      catalog_adapter_->GetEngineSystemCatalog();
  const PostgresTypeMapping* pg_type_mapping =
      engine_system_catalog->GetType(pg_type_oid);
  if (pg_type_mapping == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        const char* type_name,
        PgBootstrapCatalog::Default()->GetFormattedTypeName(pg_type_oid));
    return absl::UnimplementedError(
        absl::StrCat("The Postgres Type is not supported: ", type_name));
  }

  if (pg_type_mapping->mapped_type()) {
    return pg_type_mapping->mapped_type();
  }
  return pg_type_mapping;
}

}  // namespace postgres_translator
