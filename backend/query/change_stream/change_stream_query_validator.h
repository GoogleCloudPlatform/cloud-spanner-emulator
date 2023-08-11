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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CHANGE_STREAM_CHANGE_STREAM_QUERY_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CHANGE_STREAM_CHANGE_STREAM_QUERY_VALIDATOR_H_
#include <optional>
#include <string>
#include <vector>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/catalog/schema.h"
#include "common/constants.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Detect if a query statement is a change stream tvf query, and validate the
// arguments and syntax.
class ChangeStreamQueryValidator : public zetasql::ResolvedASTVisitor {
 public:
  struct ChangeStreamMetadata {
    ChangeStreamMetadata() { is_change_stream_query = false; }
    ChangeStreamMetadata(absl::string_view tvf_name,
                         std::vector<zetasql::Value>& args)
        : tvf_name(tvf_name) {
      start_timestamp = args[0].ToTime();
      end_timestamp = args[1].is_null()
                          ? std::optional<absl::Time>(std::nullopt)
                          : args[1].ToTime();
      partition_token = args[2].is_null()
                            ? std::optional<std::string>(std::nullopt)
                            : args[2].string_value();
      heartbeat_milliseconds = args[3].int64_value();
      change_stream_name = tvf_name.substr(5);
      partition_table =
          absl::StrCat(kChangeStreamPartitionTablePrefix, change_stream_name);
      data_table =
          absl::StrCat(kChangeStreamDataTablePrefix, change_stream_name);
      is_change_stream_query = true;
    }

    absl::Time start_timestamp;
    std::optional<absl::Time> end_timestamp;
    std::optional<std::string> partition_token;
    int64_t heartbeat_milliseconds;
    std::string partition_table;
    std::string data_table;
    std::string tvf_name;
    std::string change_stream_name;
    // If current query is not a change stream query, all the other fields will
    // be null and this bool is set to false. Vice versa, all the other fields
    // are assigned and this bool will be true.
    bool is_change_stream_query;
  };
  explicit ChangeStreamQueryValidator(
      const Schema* schema, absl::Time query_start_time,
      const absl::flat_hash_map<std::string, zetasql::Value>& params)
      : schema_(schema), query_start_time_(query_start_time), params_(params) {}

  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override {
    ZETASQL_RET_CHECK_EQ(node->node_kind(), zetasql::RESOLVED_QUERY_STMT)
        << "input is not a query statement";
    return ChangeStreamQueryValidator::ValidateQuery(node);
  }
  // Return true if current resolved query statement is a change stream tvf
  // query, return false if it is a non-tvf regular query and return an error
  // indicating Emulator does not currently support generic table valued
  // functions if it is a generic tvf query.
  absl::StatusOr<bool> IsChangeStreamQuery(const zetasql::ResolvedNode* node);

  std::string tvf_name() { return tvf_name_; }

  ChangeStreamMetadata change_stream_metadata() {
    return change_stream_metadata_;
  }

 private:
  // return Ok if query is a valid change stream tvf query.  This function will
  // only be executed if IsChangeStreamQuery returns true so we won't valid the
  // regular queries with change stream specific syntax and arguments values.
  absl::Status ValidateQuery(const zetasql::ResolvedNode* node);
  // return Ok if the tvf scan has valid syntax and arguments values.
  absl::Status ValidateTvfScanAndExtractMetadata(
      const zetasql::ResolvedTVFScan* tvf_scan);
  // return Ok if start_timestamp and end_timestamp are in valid range
  absl::Status ValidateTimeStamps(zetasql::Value start_time_value,
                                  zetasql::Value end_time_value) const;
  // return Ok if heartbeat_milliseconds is in valid range
  absl::Status ValidateHeartbeatMilliseconds(
      zetasql::Value heartbeat_value) const;
  // return Ok if read_options is null (this is an empty arguments, not
  // implemented yet in production)
  absl::Status ValidateReadOptions(zetasql::Value read_options_value) const;

  // current schema used to find the corresponding change stream and get the
  // retention period
  const Schema* schema_;
  // start time of the query, use to calculate and maximum timestamp to read
  // into the future
  const absl::Time query_start_time_;
  // parameters and their values if arguments are binded with parameters
  const absl::flat_hash_map<std::string, zetasql::Value> params_;
  // name of the tvf this validator is validating, used for outputting
  // informational error logs
  std::string tvf_name_;

  ChangeStreamMetadata change_stream_metadata_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_CHANGE_STREAM_CHANGE_STREAM_QUERY_VALIDATOR_H_
