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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUEUE_QUERYABLE_QUEUE_TVFS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUEUE_QUERYABLE_QUEUE_TVFS_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/table_valued_function.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/schema/catalog/queue.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

struct QueueReceiveMetadata {
  bool is_queue_receive_query = false;
  absl::Duration max_duration = absl::ZeroDuration();
  // Maps each leased row key to its latest delivery timestamp.
  absl::flat_hash_map<std::string, absl::Time> leased_row_keys;
  int lease_token_counter = 0;
};

class QueryableQueueReceiveTvf : public googlesql::TableValuedFunction {
 public:
  explicit QueryableQueueReceiveTvf(
      std::vector<std::string> function_name_path,
      const googlesql::FunctionSignature& signature,
      const googlesql::TVFRelation& result_schema, const Queue* queue,
      googlesql::Catalog* catalog)
      : googlesql::TableValuedFunction(
            function_name_path, /*group=*/"",
            std::vector<googlesql::FunctionSignature>{signature}),
        result_schema_(result_schema),
        queue_(queue),
        catalog_(catalog) {}

  static absl::StatusOr<std::unique_ptr<QueryableQueueReceiveTvf>> Create(
      const Queue* queue, googlesql::Catalog* catalog,
      googlesql::TypeFactory* type_factory, bool is_pg = false,
      std::vector<std::string> tvf_name_path = {});

  ~QueryableQueueReceiveTvf() override = default;

  absl::Status Resolve(
      const googlesql::AnalyzerOptions* analyzer_options,
      const std::vector<googlesql::TVFInputArgumentType>& actual_arguments,
      const googlesql::FunctionSignature& concrete_signature,
      googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory,
      std::shared_ptr<googlesql::TVFSignature>* output_tvf_signature)
      const final {
    GOOGLESQL_ASSIGN_OR_RETURN(
        *output_tvf_signature,
        googlesql::TVFSignature::Create(actual_arguments, result_schema_));
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
  CreateEvaluator(std::vector<TvfEvaluatorArg> input_arguments,
                  const std::vector<googlesql::TVFSchemaColumn>& output_columns,
                  const googlesql::FunctionSignature* function_call_signature)
      const override;

  const Queue* queue() const { return queue_; }
  googlesql::TVFRelation result_schema() const { return result_schema_; }

  void set_metadata(QueueReceiveMetadata* metadata) const {
    metadata_ = metadata;
  }

 private:
  const googlesql::TVFRelation result_schema_;
  const Queue* queue_;
  googlesql::Catalog* catalog_;
  mutable QueueReceiveMetadata* metadata_ = nullptr;
};

class QueryableQueueRenewLeaseTvf : public googlesql::TableValuedFunction {
 public:
  explicit QueryableQueueRenewLeaseTvf(
      std::vector<std::string> function_name_path,
      const googlesql::FunctionSignature& signature,
      const googlesql::TVFRelation& result_schema, const Queue* queue)
      : googlesql::TableValuedFunction(
            function_name_path, /*group=*/"",
            std::vector<googlesql::FunctionSignature>{signature}),
        result_schema_(result_schema),
        queue_(queue) {}

  static absl::StatusOr<std::unique_ptr<QueryableQueueRenewLeaseTvf>> Create(
      const Queue* queue, googlesql::TypeFactory* type_factory,
      bool is_pg = false, std::vector<std::string> tvf_name_path = {});

  ~QueryableQueueRenewLeaseTvf() override = default;

  absl::Status Resolve(
      const googlesql::AnalyzerOptions* analyzer_options,
      const std::vector<googlesql::TVFInputArgumentType>& actual_arguments,
      const googlesql::FunctionSignature& concrete_signature,
      googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory,
      std::shared_ptr<googlesql::TVFSignature>* output_tvf_signature)
      const final {
    GOOGLESQL_ASSIGN_OR_RETURN(
        *output_tvf_signature,
        googlesql::TVFSignature::Create(actual_arguments, result_schema_));
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
  CreateEvaluator(std::vector<TvfEvaluatorArg> input_arguments,
                  const std::vector<googlesql::TVFSchemaColumn>& output_columns,
                  const googlesql::FunctionSignature* function_call_signature)
      const override;

  const Queue* queue() const { return queue_; }
  googlesql::TVFRelation result_schema() const { return result_schema_; }

 private:
  const googlesql::TVFRelation result_schema_;
  const Queue* queue_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUEUE_QUERYABLE_QUEUE_TVFS_H_
