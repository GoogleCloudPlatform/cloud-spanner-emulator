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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PREPARE_PROPERTY_GRAPH_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PREPARE_PROPERTY_GRAPH_CATALOG_H_

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// This catalog is used to prepare property graphs at schema update time.
// It is not used for query time.
// PropertyGraph DDL parsing and analysis is completely delegated to ZetaSQL,
// hence this catalog is used by ZetaSQL to resolve a CREATE PROPERTY GRAPH
// statement.
class PreparePropertyGraphCatalog : public Catalog {
 public:
  PreparePropertyGraphCatalog(const Schema* schema,
                              const FunctionCatalog* function_catalog,
                              zetasql::TypeFactory* type_factory,
                              const zetasql::AnalyzerOptions& options =
                                  MakeGoogleSqlAnalyzerOptions())
      : Catalog(schema, function_catalog, type_factory, options) {}

  absl::Status GetPropertyGraph(absl::string_view name,
                                const zetasql::PropertyGraph*& property_graph,
                                const FindOptions& options) override {
    // TODO: Implement this when support for views is needed.
    return absl::OkStatus();
  }
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PREPARE_PROPERTY_GRAPH_CATALOG_H_
