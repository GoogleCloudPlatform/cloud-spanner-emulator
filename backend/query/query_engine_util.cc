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

#include "backend/query/query_engine_util.h"

#include <memory>
#include <string>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "absl/status/statusor.h"
#include "backend/query/function_catalog.h"
#include "common/errors.h"
#include "common/limits.h"
#include "third_party/spanner_pg/interface/emulator_parser.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Uses googlesql/public/analyzer to build an AnalyzerOutput for a query.
// We need to analyze the SQL before executing it in order to determine what
// kind of statement (query or DML) it is.
absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>> Analyze(
    const std::string& sql, zetasql::Catalog* catalog,
    const zetasql::AnalyzerOptions& options,
    zetasql::TypeFactory* type_factory) {
  // Check the overall length of the query string.
  if (sql.size() > limits::kMaxQueryStringSize) {
    return error::QueryStringTooLong(sql.size(), limits::kMaxQueryStringSize);
  }

  std::unique_ptr<const zetasql::AnalyzerOutput> output;
  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(sql, options, catalog,
                                              type_factory, &output));
  return output;
}

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
AnalyzePostgreSQL(const std::string& sql, zetasql::EnumerableCatalog* catalog,
                  zetasql::AnalyzerOptions& options,
                  zetasql::TypeFactory* type_factory,
                  const FunctionCatalog* function_catalog) {
  // Check the overall length of the query string.
  if (sql.size() > limits::kMaxQueryStringSize) {
    return error::QueryStringTooLong(sql.size(), limits::kMaxQueryStringSize);
  }

  options.CreateDefaultArenasIfNotSet();
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
      postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
  // PG needs ASC NULLS LAST and DESC NULLS FIRST for functions implemented as
  // a SQL rewrite.
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY);
  return postgres_translator::spangres::ParseAndAnalyzePostgreSQL(
      sql, catalog, options, type_factory,
      std::make_unique<FunctionCatalog>(
          type_factory,
          /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
          /*schema=*/function_catalog->GetLatestSchema()));
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
