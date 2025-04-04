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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_FUNCTION_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_FUNCTION_CATALOG_H_

#include <memory>
#include <string>

#include "zetasql/public/function.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::flat_hash_map<std::string, std::unique_ptr<zetasql::Function>>
GetSearchFunctions(zetasql::TypeFactory* type_factory,
                   const std::string& catalog_name,
                   database_api::DatabaseDialect dialect);

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_FUNCTION_CATALOG_H_
