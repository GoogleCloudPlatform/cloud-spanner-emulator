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

#ifndef CATALOG_SPANGRES_USER_CATALOG_H_
#define CATALOG_SPANGRES_USER_CATALOG_H_

#include <string>

#include "zetasql/public/catalog.h"
#include "absl/container/flat_hash_set.h"
#include "third_party/spanner_pg/catalog/engine_user_catalog.h"

extern const absl::flat_hash_map<std::string, std::string>
    kPgToSpannerSchemaMapping;

extern const std::vector<std::string> kUpperCaseSystemSchemaList;

namespace postgres_translator {
namespace spangres {
// SpangresUserCatalog is a derived class that holds all Spanner specific
// logic for EngineUserCatalog.
class SpangresUserCatalog : public EngineUserCatalog {
 public:
  SpangresUserCatalog(zetasql::EnumerableCatalog* engine_provided_catalog)
      : EngineUserCatalog(engine_provided_catalog, kPgToSpannerSchemaMapping,
                          kUpperCaseSystemSchemaList) {}

  const std::vector<std::string> GetCatalogPathForTable(
      const zetasql::Table* table) const override;

  absl::StatusOr<std::vector<absl::string_view>> GetPrimaryKeyColumns(
      const zetasql::Table& table) const override;

 protected:
  // Protected constructor is for the test-only subclass version,
  // spangres_test_user_catalog.
  SpangresUserCatalog(
      zetasql::EnumerableCatalog* engine_provided_catalog,
      const absl::flat_hash_map<std::string, std::string>& test_schema_map,
      const std::vector<std::string>& test_upper_case_schema_list)
      : EngineUserCatalog(engine_provided_catalog, test_schema_map,
                          test_upper_case_schema_list) {}
};
}  // namespace spangres
}  // namespace postgres_translator
#endif  // CATALOG_SPANGRES_USER_CATALOG_H_
