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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FUNCTION_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FUNCTION_CATALOG_H_

#include <memory>
#include <string>

#include "zetasql/public/function.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

constexpr char kCloudSpannerEmulatorFunctionCatalogName[] = "Spanner";

// A catalog of all SQL functions.
//
// The FunctionCatalog supports looking up a function by name and enumerating
// all existing functions.
class FunctionCatalog {
 public:
  // catalog_name allows tests to override the catalog name.
  // Overriding the catalog name is required for some PG dialect testing.
  explicit FunctionCatalog(zetasql::TypeFactory* type_factory,
                           const std::string& catalog_name =
                               kCloudSpannerEmulatorFunctionCatalogName,
                           const backend::Schema* schema = nullptr);
  void GetFunction(const std::string& name,
                   const zetasql::Function** output) const;
  void GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const;
  void GetTableValuedFunction(
      const std::string& name,
      const zetasql::TableValuedFunction** output) const;

  void SetLatestSchema(const backend::Schema* schema) {
    latest_schema_ = schema;
  }

  const backend::Schema* GetLatestSchema() const { return latest_schema_; }

 private:
  void AddZetaSQLBuiltInFunctions(zetasql::TypeFactory* type_factory);
  void AddSpannerFunctions();
  void AddMlFunctions();
  void AddSearchFunctions(zetasql::TypeFactory* type_factory);

  void AddSpannerPGFunctions(zetasql::TypeFactory* type_factory);
  void AddFunctionAliases();

  std::unique_ptr<zetasql::Function> GetInternalSequenceStateFunction(
      const std::string& catalog_name);

  std::unique_ptr<zetasql::Function> GetTableColumnIdentityStateFunction(
      const std::string& catalog_name);

  std::unique_ptr<zetasql::Function> GetNextSequenceValueFunction(
      const std::string& catalog_name);

  CaseInsensitiveStringMap<std::unique_ptr<zetasql::Function>> functions_;
  CaseInsensitiveStringMap<std::unique_ptr<zetasql::TableValuedFunction>>
      table_valued_functions_;
  const std::string catalog_name_;
  // A pointer to the latest schema, since some functions need to access it
  // (e.g. sequence functions).
  const backend::Schema* latest_schema_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FUNCTION_CATALOG_H_
