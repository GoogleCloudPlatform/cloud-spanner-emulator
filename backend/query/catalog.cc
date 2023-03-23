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

#include <memory>
#include <string>
#include <utility>

#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "backend/access/read.h"
#include "backend/query/analyzer_options.h"
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

// A sub-catalog used for resolving NET function lookups.
class NetCatalog : public zetasql::Catalog {
 public:
  explicit NetCatalog(backend::Catalog* root_catalog)
      : root_catalog_(root_catalog) {}

  static constexpr char kName[] = "NET";

  std::string FullName() const final {
    std::string name = root_catalog_->FullName();
    if (name.empty()) {
      return kName;
    }
    absl::StrAppend(&name, kName);
    return name;
  }

  // Implementation of the zetasql::Catalog interface.
  absl::Status GetFunction(const std::string& name,
                           const zetasql::Function** function,
                           const FindOptions& options) final {
    // The list of all functions is maintained in the root catalog in the
    // form of their fully-qualified names. Just prefix the function name
    // with the name of 'this' catalog and delegate the request to parent.
    return root_catalog_->GetFunction(absl::StrJoin({FullName(), name}, "."),
                                      function, options);
  }

 private:
  backend::Catalog* root_catalog_;
};

Catalog::Catalog(const Schema* schema, const FunctionCatalog* function_catalog,
                 zetasql::TypeFactory* type_factory,
                 const zetasql::AnalyzerOptions& options, RowReader* reader)
    : schema_(schema),
      function_catalog_(function_catalog),
      type_factory_(type_factory) {
  // Pass the reader to tables.
  for (const auto* table : schema->tables()) {
    tables_[table->Name()] = std::make_unique<QueryableTable>(
        table, reader, options, this, type_factory);
  }

  for (const auto* view : schema->views()) {
    views_[view->Name()] = std::make_unique<QueryableView>(view);
  }
}

absl::Status Catalog::GetCatalog(const std::string& name,
                                 zetasql::Catalog** catalog,
                                 const FindOptions& options) {
  if (absl::EqualsIgnoreCase(name, InformationSchemaCatalog::kName)) {
    *catalog = GetInformationSchemaCatalog();
  } else if (absl::EqualsIgnoreCase(name, NetCatalog::kName)) {
    *catalog = GetNetFunctionsCatalog();
  }
  return absl::OkStatus();
}

absl::Status Catalog::GetTable(const std::string& name,
                               const zetasql::Table** table,
                               const FindOptions& options) {
  *table = nullptr;
  if (auto it = views_.find(name); it != views_.end()) {
    *table = it->second.get();
    return absl::OkStatus();
  }

  if (auto it = tables_.find(name); it != tables_.end()) {
    *table = it->second.get();
    return absl::OkStatus();
  }

  return error::TableNotFound(name);
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
  output->insert(GetNetFunctionsCatalog());
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
  function_catalog_->GetFunctions(output);
  return absl::OkStatus();
}

zetasql::Catalog* Catalog::GetInformationSchemaCatalog() const {
  absl::MutexLock lock(&mu_);
  if (!information_schema_catalog_) {
    information_schema_catalog_ =
        std::make_unique<InformationSchemaCatalog>(schema_);
  }
  return information_schema_catalog_.get();
}

zetasql::Catalog* Catalog::GetNetFunctionsCatalog() const {
  absl::MutexLock lock(&mu_);
  if (!net_catalog_) {
    net_catalog_ = std::make_unique<NetCatalog>(const_cast<Catalog*>(this));
  }
  return net_catalog_.get();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
