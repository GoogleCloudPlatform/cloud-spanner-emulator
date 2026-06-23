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

#include "google/protobuf/struct.pb.h"
#include "googlesql/public/function.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

constexpr char kCloudSpannerEmulatorFunctionCatalogName[] = "Spanner";

std::unique_ptr<googlesql::Function> CreateSecureContextFunction(
    absl::string_view catalog_name,
    const absl::flat_hash_map<std::string, google::protobuf::Value>&
        secure_context);

// A catalog of all SQL functions.
//
// The FunctionCatalog supports looking up a function by name and enumerating
// all existing functions.
class FunctionCatalog {
 public:
  // catalog_name allows tests to override the catalog name.
  // Overriding the catalog name is required for some PG dialect testing.
  explicit FunctionCatalog(googlesql::TypeFactory* type_factory,
                           const std::string& catalog_name =
                               kCloudSpannerEmulatorFunctionCatalogName,
                           const backend::Schema* schema = nullptr);
  void GetFunction(const std::string& name,
                   const googlesql::Function** output) const;
  void GetFunctions(
      absl::flat_hash_set<const googlesql::Function*>* output) const;
  void GetTableValuedFunction(
      const std::string& name,
      const googlesql::TableValuedFunction** output) const;

  void SetLatestSchema(const backend::Schema* schema) {
    latest_schema_ = schema;
  }

  const backend::Schema* GetLatestSchema() const { return latest_schema_; }

 private:
  void AddGoogleSQLBuiltInFunctions(googlesql::TypeFactory* type_factory);
  void AddPGLambdaFunctions();
  void AddSpannerFunctions();
  void AddGraphSafeToJsonSignatures();
  void AddMlFunctions(googlesql::TypeFactory* type_factory);
  void AddSearchFunctions(googlesql::TypeFactory* type_factory);

  void AddSpannerPGFunctions();
  void AddFunctionAliases();

  std::unique_ptr<googlesql::Function> GetPGToCharFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetPGExtractFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetPGCastToTimestampFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetPGCastToStringFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetPGDateTruncFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetInternalSequenceStateFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetTableColumnIdentityStateFunction(
      const std::string& catalog_name);

  std::unique_ptr<googlesql::Function> GetNextSequenceValueFunction(
      const std::string& catalog_name);

  CaseInsensitiveStringMap<std::unique_ptr<googlesql::Function>> functions_;
  CaseInsensitiveStringMap<std::unique_ptr<googlesql::TableValuedFunction>>
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
