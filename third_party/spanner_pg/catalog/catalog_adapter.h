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

#ifndef SHIMS_CATALOG_ADAPTER_H_
#define SHIMS_CATALOG_ADAPTER_H_

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/engine_user_catalog.h"
#include "third_party/spanner_pg/catalog/table_name.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

// A class to store catalog information, to be used by the analyzer and
// transformer to look up info about user-generated objects.
// (e.g. oids <-> table names mappings).
//
// This CatalogAdapter class is designed to be used by the Analyzer and
// Transformer, which are both single-threaded. If these uses are ever
// changed to be multi-threaded, this class is thread-unsafe, and will
// need locking to work properly.
class CatalogAdapter {
 public:
  // Choose UINT_MAX as a starting point for table oids and then decrement
  // from there. This is relatively safe as PostgreSQL oids start from 0
  // and increase. This approach is only for development/testing purposes.
  // TODO: find a safe method to assign oids.
  static constexpr Oid kOidCounterStart = UINT_MAX;
  // Oids should be in the range for normal objects, see
  // spangres_src/src/include/access/transam.h
  static constexpr Oid kOidCounterEnd = 16384;

  // A factory method to create a `CatalogAdapter` object and let callers own
  // it. This method will fail if `catalog` is a nullptr.
  //
  // `CatalogAdapter` does not take ownership of these two engine catalogs.
  static absl::StatusOr<std::unique_ptr<CatalogAdapter>> Create(
      std::unique_ptr<EngineUserCatalog> engine_user_catalog,
      EngineSystemCatalog* engine_system_catalog,
      const zetasql::AnalyzerOptions& analyzer_options,
      absl::flat_hash_map<int, int> token_locations = {});

  // Retrieves table name from a given oid. Returns error if the oid is not
  // already tracked by the adapter.
  absl::StatusOr<TableName> GetTableNameFromOid(Oid oid) const;

  // Gets the oid of a given table. `table_name` should be canonicalized by
  // caller before calling this method (later lookup is case-sensitive).
  // Generates and saves a new oid if this table hasn't been seen before.
  //
  // If TableName is qualified, looks up the namespace Oid first. If not, uses
  // the default namespace Oid for "public" namespace (in lieu of search path).
  // TODO: Support (static) search path.
  //
  // Returns error if the next oid to be generated falls out of the range for
  // normal objects (see spangres_src/src/include/access/transam.h for more
  // details on Oid ranges).
  absl::StatusOr<Oid> GetOrGenerateOidFromTableName(
      const TableName& table_name);
  // Lower-level API that functions similarly to above, except the caller has
  // already looked up the namespace Oid. This is used to support the internal
  // PostgreSQL catalog APIs where the namespace resolution is handled
  // separately (as part of Search Path).
  absl::StatusOr<Oid> GetOrGenerateOidFromNamespaceOidAndRelationName(
      Oid namespace_oid, const std::string& relation_name);

  // Gets oid of a namespace if known, generating a new oid if unknown.
  absl::StatusOr<Oid> GetOrGenerateOidFromNamespaceName(
      const std::string& namespace_name);
  // Gets oid of a known namespace. Returns error if namespace is unknown. This
  // version of the API exists mostly to support internal PostgreSQL catalog
  // APIs, usually as part of Search Path when testing for namespace existance.
  absl::StatusOr<Oid> GetOidFromNamespaceName(
      const std::string& namespace_name);

  // Generates a new oid for a pg_proc representing a UDF from the
  // EngineUserCatalog. The passed in pg_proc struct will have its oid filled in
  // with the new oid and will be stored for later retrieval by oid. The
  // generated oid is also returned.
  // TVF is passed in as well so that the Forward Transformer can look it up by
  // oid later to avoid duplicate name resolution.
  // REQUIRED: pg_proc->oid is InvalidOid.
  // IMPORTANT: CatalogAdapter does not take ownership of the pg_proc struct.
  // The struct should be unowned and backed by MemoryContext memory.
  absl::StatusOr<Oid> GenerateAndStoreUDFProcOid(
      FormData_pg_proc* pg_proc, const zetasql::TableValuedFunction* tvf);
  // Looks up the pg_proc struct from a function oid for functions that were
  // generated from the User Catalog (UDFs, not builtins). This complements
  // bootstrap_catalog lookup for builtin function procs.
  absl::StatusOr<const FormData_pg_proc*> GetUDFProcFromOid(Oid oid) const;
  // Looks up the googlesql TableValuedFunction object that we previously
  // generated an Oid for.
  absl::StatusOr<const zetasql::TableValuedFunction*> GetTVFFromOid(
      Oid oid) const;

  EngineUserCatalog* GetEngineUserCatalog();

  EngineSystemCatalog* GetEngineSystemCatalog();

  const zetasql::AnalyzerOptions& analyzer_options() const {
    return analyzer_options_;
  }

  const absl::flat_hash_map<int, int>& token_locations() const {
    return token_locations_;
  }

  int max_column_id() const { return max_column_id_; }

  // Returns a unique column id, used to build a ZetaSQL resolved AST.
  absl::StatusOr<int> AllocateColumnId();

 private:
  // CatalogAdapter takes ownership of the engine user catalog, which is a
  // wrapper around the underlying catalog that remains owned by the storage
  // engine. CatalogAdapter does not take ownership of the engine system
  // catalog, which is a singleton that is reused for all Spangres queries.
  explicit CatalogAdapter(
      std::unique_ptr<EngineUserCatalog> engine_user_catalog,
      EngineSystemCatalog* engine_system_catalog,
      const zetasql::AnalyzerOptions& analyzer_options,
      absl::flat_hash_map<int, int> token_locations);

  // Looks up namespace name from Oid. Helper used when producing a TableName
  // object to return.
  absl::StatusOr<std::string> GetNamespaceNameFromOid(Oid oid) const;

  // Generates a new oid.
  absl::StatusOr<Oid> GenerateOid();

  // Adds a table and oid to the adapter to track.
  // Returns error if either of them is already in the maps.
  absl::Status AddOidAndTableNameToMaps(Oid schema_oid, Oid table_oid,
                                        const std::string& relation_name);

  // Adds a namespace and oid to the adapter to track.
  // Returns error if either of them is already in the maps.
  absl::Status AddOidAndNamespaceNameToMaps(Oid namespace_oid,
                                            const std::string& namespace_name);

  //
  // Member variables to track assignment and generation of Oids.
  //

  // Maps to track synthetic oids generated for tables and namespaces.
  //
  // Tables are identified by the pair of <namespace_oid, unqualified_name>.
  // When the table name was unqualified in the query it is assumed to mean
  // "public" in lieu of search path. Table names inside these maps should
  // already be canonicalized.
  absl::flat_hash_map<Oid, std::pair<Oid, std::string>> oid_to_table_map_;
  absl::flat_hash_map<std::pair<Oid, std::string>, Oid> table_to_oid_map_;
  // Namespace maps function like above.
  // Note that catalog_name a.k.a. database_name is not supported.
  absl::flat_hash_map<std::string, Oid> namespace_to_oid_map_;
  absl::flat_hash_map<Oid, std::string> oid_to_namespace_map_;
  // Like above but for FormData_pg_proc structs generated from user catalog
  // functions (currently only Change Stream TVFs). The structs themselves
  // are unowned and live in the MemoryContext created for this query.
  // Procs contain their oids, and name resolution is handled in the analyzer,
  // so a bidirectional map is not needed here.
  absl::flat_hash_map<Oid, const FormData_pg_proc*> oid_to_udf_proc_map_;
  absl::flat_hash_map<Oid, const zetasql::TableValuedFunction*>
      oid_to_tvf_map_;

  // A counter to assign unique oids to RTEs.
  // TODO: find a safe method to assign oids.
  // Note: this is -1 only for historical reasons to preserve test outputs.
  Oid oid_counter_ = kOidCounterStart - 1;

  //
  // All other (non-Oid) member variables for any state that lives longer than
  // the Forward Transformer itself.
  //

  // Engine-specific catalogs to be used by the analyzer and transformer.
  std::unique_ptr<EngineUserCatalog> engine_user_catalog_;
  EngineSystemCatalog* engine_system_catalog_ = nullptr;  // Not owned.

  // ZetaSQL AnalyzerOptions determines certain characteristics of the
  // returned Resolved AST and contains a memory arena for allocating IdStrings.
  // The AnalyzerOptions also include LanguageOptions which are used for
  // function matching and error message type names.
  const zetasql::AnalyzerOptions& analyzer_options_;  // Not owned.

  // Maps from start to end positions of tokens in the SQL expression
  absl::flat_hash_map<int, int> token_locations_;

  // Tracks the maximum column id allocated so far. This also serves as a
  // counter to help assign new column ids.
  int max_column_id_ = 0;
};

}  // namespace postgres_translator

#endif  // SHIMS_CATALOG_ADAPTER_H_
