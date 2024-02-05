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

#ifndef BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_ACCESSOR_H_
#define BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_ACCESSOR_H_

#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"

namespace postgres_translator {

class PgBootstrapCatalog;

// Returns a pointer to the Spangres Bootstrap Catalog which contains metadata
// about PG built-ins.
const PgBootstrapCatalog* GetPgBootstrapCatalog();

// Returns either a struct containing collation data or an error based on the
// given collation_name. If collation_name is not an allowed collation (see
// allowlist) or is not in the catalog, returns NotFoundError.
absl::StatusOr<PgCollationData> GetPgCollationDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view collation_name);

// Returns either a struct containing namespace data or an error based on the
// given namespace_name. If the namespace is not an allowed type (see allowlist)
// or is not in the catalog, return NotFoundError.
absl::StatusOr<PgNamespaceData> GetPgNamespaceDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view namespace_name);

// Returns either a struct containing type data or an error based on the given
// type_name. The type name should use the PG format e.g. "jsonb", "_jsonb".
// If type_name is not an allowed type (see allowlist) or is not in the catalog,
// returns NotFoundError.
absl::StatusOr<PgTypeData> GetPgTypeDataFromBootstrap(
    const PgBootstrapCatalog* catalog, absl::string_view type_name);

// Returns either a struct containing type data or an error based on the given
// type oid. If the type associated with the oid is not an allowed type
// (see allowlist) or is not in the catalog or is not a type oid, returns
// NotFoundError.
absl::StatusOr<PgTypeData> GetPgTypeDataFromBootstrap(
    const PgBootstrapCatalog* catalog, int64_t type_oid);

}  // namespace postgres_translator

#endif  // BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_ACCESSOR_H_
