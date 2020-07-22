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
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "backend/common/case.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A catalog of all SQL functions.
//
// The FunctionCatalog supports looking up a function by name and emunerating
// all existing functions.
class FunctionCatalog {
 public:
  explicit FunctionCatalog(zetasql::TypeFactory* type_factory);
  void GetFunction(const std::string& name,
                   const zetasql::Function** output) const;
  void GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const;

 private:
  void AddZetaSQLBuiltInFunctions(zetasql::TypeFactory* type_factory);

  void AddSpannerFunctions();

  void AddFunctionAliases();

  CaseInsensitiveStringMap<std::unique_ptr<zetasql::Function>> functions_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FUNCTION_CATALOG_H_
