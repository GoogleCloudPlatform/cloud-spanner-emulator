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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CHANGE_STREAM_QUERYABLE_CHANGE_STREAM_TVF_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CHANGE_STREAM_QUERYABLE_CHANGE_STREAM_TVF_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/table_valued_function.h"
#include "backend/schema/catalog/change_stream.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
class QueryableChangeStreamTvf : public zetasql::TableValuedFunction {
 public:
  explicit QueryableChangeStreamTvf(
      std::vector<std::string> function_name_path,
      const zetasql::FunctionSignature& signature,
      const zetasql::TVFRelation& result_schema,
      const backend::ChangeStream* change_stream)
      : zetasql::TableValuedFunction(function_name_path, signature),
        result_schema_(result_schema) {}

  static absl::StatusOr<std::unique_ptr<QueryableChangeStreamTvf>> Create(
      const backend::ChangeStream* change_stream,
      zetasql::AnalyzerOptions options, zetasql::Catalog* catalog,
      zetasql::TypeFactory* type_factory, bool is_pg);

  static constexpr char kChangeStreamTvfStructPrefix[] = "READ_";
  static constexpr char kChangeStreamTvfJsonPrefix[] = "read_json_";
  static constexpr char kChangeStreamTvfStartTimestamp[] = "start_timestamp";
  static constexpr char kChangeStreamTvfEndTimestamp[] = "end_timestamp";
  static constexpr char kChangeStreamTvfPartitionToken[] = "partition_token";
  static constexpr char kChangeStreamTvfReadOptions[] = "read_options";
  static constexpr char kChangeStreamTvfHeartbeatMilliseconds[] =
      "heartbeat_milliseconds";
  ~QueryableChangeStreamTvf() override = default;

  // Resolves the output schema which is fixed for change stream TVFs.
  // See details at `zetasql::TableValuedFunction`
  absl::Status Resolve(
      const zetasql::AnalyzerOptions* analyzer_options,
      const std::vector<zetasql::TVFInputArgumentType>& actual_arguments,
      const zetasql::FunctionSignature& concrete_signature,
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      std::shared_ptr<zetasql::TVFSignature>* output_tvf_signature)
      const final {
    output_tvf_signature->reset(
        new zetasql::TVFSignature(actual_arguments, result_schema_));
    return absl::OkStatus();
  }

  zetasql::TVFRelation result_schema() const { return result_schema_; }

 private:
  // Output schema of the TVF.
  const zetasql::TVFRelation result_schema_;
};
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CHANGE_STREAM_QUERYABLE_CHANGE_STREAM_TVF_H_
