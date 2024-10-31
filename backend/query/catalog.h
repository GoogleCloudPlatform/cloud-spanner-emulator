//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CATALOG_H_

#include <memory>
#include <optional>
#include <string>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/common/case.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/function_catalog.h"
#include "backend/query/queryable_model.h"
#include "backend/query/queryable_named_schema.h"
#include "backend/query/queryable_sequence.h"
#include "backend/query/queryable_table.h"
#include "backend/query/queryable_view.h"
#include "backend/query/spanner_sys_catalog.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class NetCatalog;
class PGFunctionCatalog;

// Implementation of zetasql::Catalog for the root catalog in the catalog
// hierarchy. For more details, see code of zetasql::Catalog.
class Catalog : public zetasql::EnumerableCatalog {
 public:
  // 'reader' can be nullptr unless CreateEvaluatorTableIterator is called
  // on tables in the catalog.
  Catalog(
      const Schema* schema, const FunctionCatalog* function_catalog,
      zetasql::TypeFactory* type_factory,
      const zetasql::AnalyzerOptions& options =
          MakeGoogleSqlAnalyzerOptions(),
      RowReader* reader = nullptr, QueryEvaluator* query_evaluator = nullptr,
      std::optional<std::string> change_stream_internal_lookup = std::nullopt);

  std::string FullName() const override {
    // The name of the root catalog is "".
    return "";
  }

 private:
  friend class NetCatalog;
  friend class PGFunctionCatalog;
  // These tests needs to access the tvf map and manually add an empty tvf.
  FRIEND_TEST(ChangeStreamQueryValidatorTest,
              ValidateNoneChangeStreamTvfWithSamePrefixIsFiltered);
  FRIEND_TEST(ChangeStreamQueryValidatorTest,
              ValidateNoneChangeStreamTvfWithDifferentPrefixIsFiltered);

  // Implementation of the zetasql::Catalog interface.
  absl::Status GetCatalog(const std::string& name, zetasql::Catalog** catalog,
                          const FindOptions& options) override;
  absl::Status GetTable(const std::string& name, const zetasql::Table** table,
                        const FindOptions& options) override;
  absl::Status GetModel(const std::string& name, const zetasql::Model** model,
                        const FindOptions& options = FindOptions()) override;
  absl::Status GetFunction(const std::string& name,
                           const zetasql::Function** function,
                           const FindOptions& options) override;
  absl::Status GetProcedure(const std::string& full_name,
                            const zetasql::Procedure** procedure,
                            const FindOptions& options) final;
  absl::Status FindTableValuedFunction(
      const absl::Span<const std::string>& path,
      const zetasql::TableValuedFunction** function,
      const FindOptions& options = FindOptions()) final;
  absl::Status GetSequence(const std::string& name,
                           const zetasql::Sequence** sequence,
                           const FindOptions& options) override;

  // Implementation of the zetasql::EnumerableCatalog interface.
  absl::Status GetCatalogs(
      absl::flat_hash_set<const zetasql::Catalog*>* output) const final;
  absl::Status GetTables(
      absl::flat_hash_set<const zetasql::Table*>* output) const final;
  absl::Status GetTypes(
      absl::flat_hash_set<const zetasql::Type*>* output) const final;
  absl::Status GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const final;

  absl::Status GetType(const std::string& name, const zetasql::Type** type,
                       const FindOptions& options) final;

  absl::Status PopulateSystemProcedureMap();

  // Returns the information schema catalog (creating one if needed).
  zetasql::Catalog* GetInformationSchemaCatalog() const
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the PG information schema catalog (creating one if needed).
  zetasql::Catalog* GetPGInformationSchemaCatalog() const
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the spanner sys catalog (creating one if needed).
  SpannerSysCatalog* GetSpannerSysCatalog() const ABSL_LOCKS_EXCLUDED(mu_);
  // Returns the spanner sys catalog (creating one if needed).
  SpannerSysCatalog* GetSpannerSysCatalogWithoutLocks() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Returns the NET catalog.
  zetasql::Catalog* GetNetFunctionsCatalog() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the PG functions catalog.
  zetasql::Catalog* GetPGFunctionsCatalog() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the PG catalog (similar to information_schema).
  zetasql::Catalog* GetPGCatalog() const ABSL_LOCKS_EXCLUDED(mu_);

  QueryableNamedSchema* GetNamedSchema(const std::string& name);

  // Returns all named schemas
  absl::Status GetNamedSchemas(
      absl::flat_hash_set<const zetasql::Catalog*>* output) const;

  // Adds a schema object to the named schema.
  template <typename T>
  absl::Status AddObjectToNamedSchema(const std::string& named_schema_name,
                                      T object);

  // The backend schema (which is the default schema in this catalog).
  const Schema* schema_ = nullptr;

  // Tables available in the default schema.
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableTable>> tables_;
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableView>> views_;
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableModel>> models_;

  // Types available in the default schema.
  CaseInsensitiveStringMap<const zetasql::Type*> types_;

  // Change Stream TVFs available in the default schema.
  CaseInsensitiveStringMap<
      std::unique_ptr<const zetasql::TableValuedFunction>>
      tvfs_;

  // Sequences available in the default schema.
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableSequence>> sequences_;

  // Named schemas available in the default schema.
  CaseInsensitiveStringMap<std::unique_ptr<QueryableNamedSchema>>
      named_schemas_;

  // Functions available in the default schema.
  const FunctionCatalog* function_catalog_ = nullptr;
  zetasql::TypeFactory* type_factory_ = nullptr;

  // Callback used to evaluate queries. May be unset for queries that
  // do not involve views.
  QueryEvaluator* query_evaluator_ = nullptr;

  // Mutex to protect state below.
  mutable absl::Mutex mu_;

  // Information schema catalog (created only if accessed).
  mutable std::unique_ptr<zetasql::Catalog> information_schema_catalog_
      ABSL_GUARDED_BY(mu_);

  // PG information schema catalog (created only if accessed).
  mutable std::unique_ptr<zetasql::Catalog> pg_information_schema_catalog_
      ABSL_GUARDED_BY(mu_);

  // Spanner sys catalog (created only if accessed).
  mutable std::unique_ptr<SpannerSysCatalog> spanner_sys_catalog_
      ABSL_GUARDED_BY(mu_);

  // Sub-catalog for resolving NET function lookup.
  mutable std::unique_ptr<zetasql::Catalog> net_catalog_ ABSL_GUARDED_BY(mu_);

  // Sub-catalog for resolving PG function lookup.
  mutable std::unique_ptr<zetasql::Catalog> pg_function_catalog_
      ABSL_GUARDED_BY(mu_);

  // Sub-catalog for resolving pg_catalog lookup.
  mutable std::unique_ptr<zetasql::Catalog> pg_catalog_ ABSL_GUARDED_BY(mu_);

  // System Procedures available.
  CaseInsensitiveStringMap<std::unique_ptr<zetasql::Procedure>> procedures_;

  // User defined functions available.
  CaseInsensitiveStringMap<std::unique_ptr<zetasql::Function>> udfs_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CATALOG_H_
