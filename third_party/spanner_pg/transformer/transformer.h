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

#ifndef TRANSFORMER_TRANSFORMER_H_
#define TRANSFORMER_TRANSFORMER_H_

#include <map>
#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

class Transformer {
 public:
  // Builds a `RangeTblEntry` object. This function should produce the same
  // RangeTblEntry as the function addRangeTableEntry in PostgreSQL.
  //
  // If we are coming from a pg query, this function expects an OID be provided.
  // If we started from a RangeVar, then we would generate an OID for that name
  // from the catalog adapter.
  //
  // If we are coming from a googlesql query, then the resolved table will have
  // its name reverse-translated then turned into an OID from the catalog
  // adapter.
  //
  // This function is static so that the analyzer can use it without defining
  // a Transformer instance.
  static absl::StatusOr<RangeTblEntry*> BuildPgRangeTblEntry(
      CatalogAdapter& adapter, const zetasql::Table& resolved_table,
      Alias* alias, bool inFromCl, AclMode acl_mode);
  static absl::StatusOr<RangeTblEntry*> BuildPgRangeTblEntry(
      CatalogAdapter& adapter, Oid oid, Alias* alias, bool inFromCl,
      AclMode acl_mode);

  static absl::StatusOr<Oid> BuildPgTypeOid(CatalogAdapter& catalog_adapter,
                                            const zetasql::Type* gsql_type);

  // Given a map of parameter name -> ZetaSQL type, get the parameters in
  // order and transform each parameter type from a ZetaSQL type to a
  // PostgreSQL type. Return an error if any parameter names don't match the
  // expected naming scheme (p1, p2, p3, etc).
  // Set max_param to the largest parameter number seen, which may differ
  // from the size of gsql_param_types if there are missing parameter types.
  static absl::StatusOr<Oid*> BuildPgParameterTypeList(
      CatalogAdapter& catalog_adapter,
      const std::map<std::string, const zetasql::Type*>& gsql_param_types,
      int* max_param);
};

}  // namespace postgres_translator

#endif  // TRANSFORMER_TRANSFORMER_H_
