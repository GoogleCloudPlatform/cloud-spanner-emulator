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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_NAMED_SCHEMA_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_NAMED_SCHEMA_H_

#include <memory>
#include <string>
#include <utility>

#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/common/case.h"
#include "backend/query/function_catalog.h"
#include "backend/query/queryable_model.h"
#include "backend/query/queryable_sequence.h"
#include "backend/query/queryable_table.h"
#include "backend/query/queryable_view.h"
#include "backend/schema/catalog/named_schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A wrapper over NamedSchema class which implements
// zetasql::EnumerableCatalog. QueryableNamedSchema has a reference to the
// backend NamedSchema.
class QueryableNamedSchema : public zetasql::EnumerableCatalog {
 public:
  explicit QueryableNamedSchema(
      const backend::NamedSchema* backend_named_schema);

  const backend::NamedSchema* wrapped_named_schema() const {
    return wrapped_named_schema_;
  }

  std::string Name() const { return wrapped_named_schema_->Name(); }

  // FullName is will be the same as name since the emulator only allows one
  // layer of named schemas.
  std::string FullName() const override { return Name(); }

  void AddObject(std::unique_ptr<const QueryableSequence> sequence) {
    sequences_[sequence->Name()] = std::move(sequence);
  }

  void AddObject(std::unique_ptr<const QueryableTable> table) {
    tables_[table->Name()] = std::move(table);
  }

  void AddObject(std::unique_ptr<const QueryableView> view) {
    views_[view->Name()] = std::move(view);
  }

  void AddObject(std::unique_ptr<const QueryableModel> model) {
    models_[model->Name()] = std::move(model);
  }

  void AddObject(std::unique_ptr<const zetasql::Function> function) {
    udfs_[function->Name()] = std::move(function);
  }

  // Implementation of the zetasql::EnumerableCatalog interface.
  absl::Status GetTables(
      absl::flat_hash_set<const zetasql::Table*>* output) const final;
  absl::Status GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const final;
  absl::Status GetCatalogs(
      absl::flat_hash_set<const zetasql::Catalog*>* output) const final;
  absl::Status GetTypes(
      absl::flat_hash_set<const zetasql::Type*>* output) const final;

  absl::Status GetType(const std::string& name, const zetasql::Type** type,
                       const FindOptions& options) final;
  absl::Status GetTable(const std::string& name, const zetasql::Table** table,
                        const FindOptions& options) final;
  absl::Status GetModel(const std::string& name, const zetasql::Model** model,
                        const FindOptions& options) final;
  absl::Status GetSequence(const std::string& name,
                           const zetasql::Sequence** sequence,
                           const FindOptions& options) final;
  absl::Status GetFunction(const std::string& name,
                           const zetasql::Function** function,
                           const FindOptions& options) final;

 private:
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableTable>> tables_;
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableSequence>> sequences_;
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableView>> views_;
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableModel>> models_;
  CaseInsensitiveStringMap<std::unique_ptr<const zetasql::Function>> udfs_;

  // Functions available in the default schema.
  const FunctionCatalog* function_catalog_ = nullptr;
  zetasql::TypeFactory* type_factory_ = nullptr;

  // The underlying NamedSchema object which backs the QueryableNamedSchema.
  const backend::NamedSchema* wrapped_named_schema_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_NAMED_SCHEMA_H_
