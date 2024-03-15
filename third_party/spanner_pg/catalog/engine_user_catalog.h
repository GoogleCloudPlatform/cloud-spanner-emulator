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

#ifndef CATALOG_ENGINE_USER_CATALOG_H_
#define CATALOG_ENGINE_USER_CATALOG_H_

#include "zetasql/base/logging.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/base/case.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/die_if_null.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/catalog/table_name.h"

namespace postgres_translator {
// A derived class of EnumerableCatalog. This enables us to map identifiers in
// user queries to different objects in the GsqlCatalog e.g., we use it to map
// information_schema in queries to pg_information_schema in the GsqlCatalog.
class EngineUserCatalog : public zetasql::EnumerableCatalog {
 public:
  constexpr static absl::string_view kPublicSchema = "public";

  // Does not take ownership of the `engine_provided_catalog` which must outlive
  // this object.
  explicit EngineUserCatalog(
      zetasql::EnumerableCatalog* engine_provided_catalog,
      const absl::flat_hash_map<std::string, std::string>& pg_to_engine_schema,
      const std::vector<std::string>& uppercase_schema_list)
      : engine_provided_catalog_(ZETASQL_DIE_IF_NULL(engine_provided_catalog)),
        uppercase_schema_list_(uppercase_schema_list) {
    for (const auto& entry : pg_to_engine_schema) {
      blocked_schemas_.emplace(entry.second);
      // The comment in pg_to_engine_schema_ defintion explains why we convert
      // the keys to lowercase.
      pg_to_engine_schema_.emplace(absl::AsciiStrToLower(entry.first),
                                   entry.second);
      engine_to_pg_schema_.emplace(entry.second,
                                   absl::AsciiStrToLower(entry.first));
    }
  }

  std::string FullName() const override;

  // FindTable uses the pg_to_engine_schema_ and blocked_schemas_ member
  // variables to map between the requested path and the actual path.
  absl::Status FindTable(const absl::Span<const std::string>& path,
                         const zetasql::Table** table,
                         const FindOptions& options = FindOptions()) override;

  // FindTableValuedFunction uses the engine-specific IsUserDefinedTVF to filter
  // out non-UDFs from the result to prevent duplicates with the SystemCatalog.
  absl::Status FindTableValuedFunction(
      const absl::Span<const std::string>& path,
      const zetasql::TableValuedFunction** tvf,
      const FindOptions& options = FindOptions()) override;

  // InUppercaseCatalogPath uses GetCatalogPathForTable() to check if a table's
  // schema is in the uppercase_schema_list_.
  absl::StatusOr<bool> InUppercaseCatalogPath(
      const zetasql::Table* gsql_table) const;

  virtual const std::vector<std::string> GetCatalogPathForTable(
      const zetasql::Table* table) const = 0;

  virtual absl::StatusOr<std::vector<absl::string_view>> GetPrimaryKeyColumns(
      const zetasql::Table& table) const = 0;

  // The methods below use the implementations of the held
  // engine_provided_catalog object.
  absl::Status GetCatalogs(absl::flat_hash_set<const zetasql::Catalog*>*
                               output) const override final {
    return engine_provided_catalog_->GetCatalogs(output);
  };
  absl::Status GetTables(absl::flat_hash_set<const zetasql::Table*>* output)
      const override final {
    return engine_provided_catalog_->GetTables(output);
  }
  absl::Status GetTypes(absl::flat_hash_set<const zetasql::Type*>* output)
      const override final {
    return engine_provided_catalog_->GetTypes(output);
  }
  absl::Status GetFunctions(absl::flat_hash_set<const zetasql::Function*>*
                                output) const override final {
    return engine_provided_catalog_->GetFunctions(output);
  }
  absl::Status GetConversions(absl::flat_hash_set<const zetasql::Conversion*>*
                                  output) const override final {
    return engine_provided_catalog_->GetConversions(output);
  }

  // Return the postgres TableName for the given googlesql table. The FullName
  // as provided by the googlesql table would have had its schema name mapped
  // by MapSchemaName, so we need to invert that map.
  absl::StatusOr<TableName> GetTableNameForGsqlTable(
      const zetasql::Table& table) const;

 private:
  friend class EngineUserCatalogTestPeer;

  absl::StatusOr<std::string> MapSchemaName(
      std::string schema_name, const absl::Span<const std::string>& path) const;
  zetasql::EnumerableCatalog* engine_provided_catalog_;
  // Mapping of PG schema names (which MUST be lowercased) to
  // googlesql names (which are case insensitive).
  // PG schema names (i.e., the map keys) should be lowercased because:
  //  * PG system schemas have lowercase names
  //  * Unlike googlesql, PG schema & table lookup is case-sensitive, so we
  //    perform case sensitive lookup against this map.
  //  * unquoted identifiers in user queries get converted to lowercase before
  //    calling this catalog.
  // `pg_to_engine_schema_` is set by the derived engine class.
  absl::flat_hash_map<std::string, std::string> pg_to_engine_schema_;
  absl::flat_hash_map<std::string, std::string> engine_to_pg_schema_;
  // Contains internal schema names which should not be directly accessible
  // to PG users. Should contain the values in `pg_to_engine_schema_` map.
  // Although Pg is case-sensitive, we perform case-insensitive check for
  // blocked schemas because the underlying zetasql::Catalog::FindTable is
  // case-insensitive.
  absl::btree_set<absl::string_view, zetasql_base::CaseLess>
      blocked_schemas_;
  // `uppercase_schema_list` is set by the derived engine class.
  std::vector<std::string> uppercase_schema_list_;
};

}  // namespace postgres_translator

#endif  // CATALOG_ENGINE_USER_CATALOG_H_
