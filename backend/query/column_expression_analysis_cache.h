//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_COLUMN_EXPRESSION_ANALYSIS_CACHE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_COLUMN_EXPRESSION_ANALYSIS_CACHE_H_

#include <cstddef>
#include <memory>
#include <string>

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "backend/schema/catalog/column.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Analyzed default and generated column expressions, shared across the
// statements of one query engine.
//
// A catalog is built for every statement, and building it analyzes the
// expression of every default and generated column in the schema; with a few
// dozen such columns that was most of the fixed cost of a statement. The
// analysis depends only on the column, its expression, its dependent columns'
// types and the default time zone, so it is kept here, keyed by exactly those,
// and reused by every later catalog.
//
// The cache belongs to the engine's FunctionCatalog, because a resolved
// expression points at that catalog's functions and must not outlive them. An
// expression that resolves to a sequence, a SQL UDF or a subquery is never
// cached: its resolved tree would point at objects the per-statement catalog
// owns.
class ColumnExpressionAnalysisCache {
 public:
  static std::string KeyFor(const Column* column,
                            const googlesql::AnalyzerOptions& options);

  std::shared_ptr<const googlesql::AnalyzerOutput> Find(
      const std::string& key) ABSL_LOCKS_EXCLUDED(mu_);

  void Store(const std::string& key,
             std::shared_ptr<const googlesql::AnalyzerOutput> output)
      ABSL_LOCKS_EXCLUDED(mu_);

 private:
  static constexpr size_t kMaxEntries = 10000;

  static bool IsShareable(const googlesql::AnalyzerOutput& output);

  absl::Mutex mu_;
  absl::flat_hash_map<std::string,
                      std::shared_ptr<const googlesql::AnalyzerOutput>>
      entries_ ABSL_GUARDED_BY(mu_);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_COLUMN_EXPRESSION_ANALYSIS_CACHE_H_
