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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_UTIL_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_UTIL_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/type.h"
#include "absl/status/statusor.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/query/function_catalog.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

struct ExecuteUpdateResult {
  // A mutation which describes the set of data changes required by the DML
  // statement.
  Mutation mutation;
  // Number of rows that were modified.
  int64_t modify_row_count;
  // Output row cursor from THEN RETURN clause. This row cursor pointer will be
  // NULL if there are no output rows like regular DMLs.
  std::unique_ptr<RowCursor> returning_row_cursor;
};

// INSERT ON CONFLICT DML execution can potentially run two queries INSERT and
// UPDATE DML. This struct collects the returning rows from each statement
// execution. A single row cursor with returning results from both the queries
// is returned at the end of ON CONFLICT DML execution.
struct InsertOnConflictReturningRows {
  std::vector<std::string> column_names;
  std::vector<const zetasql::Type*> column_types;
  std::vector<std::vector<zetasql::Value>> rows;
};

// Uses googlesql/public/analyzer to build an AnalyzerOutput for a query.
// We need to analyze the SQL before executing it in order to determine what
// kind of statement (query or DML) it is.
absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>> Analyze(
    const std::string& sql, zetasql::Catalog* catalog,
    const zetasql::AnalyzerOptions& options,
    zetasql::TypeFactory* type_factory);

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
AnalyzePostgreSQL(const std::string& sql, zetasql::EnumerableCatalog* catalog,
                  zetasql::AnalyzerOptions& options,
                  zetasql::TypeFactory* type_factory,
                  const FunctionCatalog* function_catalog);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_UTIL_H_
