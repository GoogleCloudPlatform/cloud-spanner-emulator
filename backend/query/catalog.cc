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

#include "backend/query/catalog.h"

#include "zetasql/public/function.h"
#include "absl/memory/memory.h"
#include "absl/strings/match.h"
#include "backend/access/read.h"
#include "backend/query/function_catalog.h"
#include "backend/query/information_schema_catalog.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/schema.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

Catalog::Catalog(const Schema* schema, const FunctionCatalog* function_catalog,
                 RowReader* reader)
    : schema_(schema), function_catalog_(function_catalog) {
  for (const auto* table : schema->tables()) {
    tables_[table->Name()] = absl::make_unique<QueryableTable>(table, reader);
  }
}

absl::Status Catalog::GetCatalog(const std::string& name,
                                 zetasql::Catalog** catalog,
                                 const FindOptions& options) {
  if (absl::EqualsIgnoreCase(name, InformationSchemaCatalog::kName)) {
    *catalog = GetInformationSchemaCatalog();
  } else {
    *catalog = nullptr;
  }
  return ::absl::OkStatus();
}

absl::Status Catalog::GetTable(const std::string& name,
                               const zetasql::Table** table,
                               const FindOptions& options) {
  auto iter = tables_.find(name);
  if (iter == tables_.end()) {
    *table = nullptr;
    return error::TableNotFound(name);
  } else {
    *table = iter->second.get();
    return absl::OkStatus();
  }
}

absl::Status Catalog::GetFunction(const std::string& name,
                                  const zetasql::Function** function,
                                  const FindOptions& options) {
  function_catalog_->GetFunction(name, function);
  return absl::OkStatus();
}

absl::Status Catalog::GetCatalogs(
    absl::flat_hash_set<const zetasql::Catalog*>* output) const {
  output->insert(GetInformationSchemaCatalog());
  return absl::OkStatus();
}

absl::Status Catalog::GetTables(
    absl::flat_hash_set<const zetasql::Table*>* output) const {
  for (auto iter = tables_.begin(); iter != tables_.end(); ++iter) {
    output->insert(iter->second.get());
  }
  return absl::OkStatus();
}
absl::Status Catalog::GetTypes(
    absl::flat_hash_set<const zetasql::Type*>* output) const {
  // Currently, Cloud Spanner doesn't support proto or enum types.
  return absl::OkStatus();
}
absl::Status Catalog::GetFunctions(
    absl::flat_hash_set<const zetasql::Function*>* output) const {
  // TODO: Add functions when we have implemented an AST filter
  // that returns a canonical, supported list of functions.
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Catalog::GetFunctions is not implemented");
}

zetasql::Catalog* Catalog::GetInformationSchemaCatalog() const {
  absl::MutexLock lock(&mu_);
  if (!information_schema_catalog_) {
    information_schema_catalog_ =
        absl::make_unique<InformationSchemaCatalog>(schema_);
  }
  return information_schema_catalog_.get();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
