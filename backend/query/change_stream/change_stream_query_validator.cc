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

#include "backend/query/change_stream/change_stream_query_validator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/time/time.h"
#include "backend/schema/catalog/change_stream.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status ChangeStreamQueryValidator::ValidateReadOptions(
    const googlesql::Value read_options_value) const {
  if (!read_options_value.is_null()) {
    return error::InvalidChangeStreamTvfArgumentNonNullReadOptions();
  }
  return absl::OkStatus();
}
absl::Status ChangeStreamQueryValidator::ValidateHeartbeatMilliseconds(
    const googlesql::Value heartbeat_value) const {
  if (heartbeat_value.is_null()) {
    return error::InvalidChangeStreamTvfArgumentNullHeartbeat();
  }
  int64_t heartbeat_num = heartbeat_value.ToInt64();
  if (heartbeat_num > limits::kChangeStreamsMaxHeartbeatMilliseconds ||
      heartbeat_num < limits::kChangeStreamsMinHeartbeatMilliseconds) {
    return error::InvalidChangeStreamTvfArgumentOutOfRangeHeartbeat(
        limits::kChangeStreamsMinHeartbeatMilliseconds,
        limits::kChangeStreamsMaxHeartbeatMilliseconds, heartbeat_num);
  }
  return absl::OkStatus();
}

absl::Status ChangeStreamQueryValidator::ValidateTimeStamps(
    const googlesql::Value start_time_value,
    const googlesql::Value end_time_value) const {
  if (start_time_value.is_null()) {
    return error::InvalidChangeStreamTvfArgumentNullStartTimestamp();
  }

  const ChangeStream* change_stream =
      schema_->FindChangeStream(change_stream_name_);
  absl::Time max_time =
      query_start_time_ +
      absl::Minutes(limits::kChangeStreamsMaxStartTimestampDelay);
  absl::Time min_time =
      std::max(query_start_time_ -
                   absl::Seconds(change_stream->parsed_retention_period()),
               change_stream->creation_time());
  absl::Time start_time = start_time_value.ToTime();

  if (start_time < min_time) {
    return error::InvalidChangeStreamTvfArgumentStartTimestampTooOld(
        absl::FormatTime(min_time), absl::FormatTime(start_time));
  }
  if (start_time > max_time) {
    return error::InvalidChangeStreamTvfArgumentStartTimestampTooFarInFuture(
        absl::FormatTime(min_time), absl::FormatTime(max_time),
        absl::FormatTime(start_time));
  }

  if (change_stream->partition_mode() ==
      kChangeStreamPartitionModeMutableKeyRange) {
    if (end_time_value.is_null()) {
      return error::InvalidChangeStreamTvfArgumentNullEndTimestamp();
    }
    absl::Time end_time = end_time_value.ToTime();
    absl::Time max_end_time =
        start_time + absl::Minutes(limits::kChangeStreamsMaxEndTimestampDelay);
    if (end_time > max_end_time) {
      return error::InvalidChangeStreamTvfArgumentEndTimestampTooFarInFuture(
          absl::FormatTime(start_time), absl::FormatTime(max_end_time),
          absl::FormatTime(end_time));
    }
  }

  if (!end_time_value.is_null()) {
    absl::Time end_time = end_time_value.ToTime();
    if (start_time > end_time) {
      return error::
          InvalidChangeStreamTvfArgumentStartTimestampGreaterThanEndTimestamp(
              absl::FormatTime(start_time), absl::FormatTime(end_time));
    }
  }
  return absl::OkStatus();
}

absl::Status ChangeStreamQueryValidator::ValidateTvfScanAndExtractMetadata(
    const googlesql::ResolvedTVFScan* tvf_scan) {
  absl::flat_hash_map<std::string, const googlesql::ResolvedLiteral*> arg_map;
  if (!tvf_scan->alias().empty()) {  // prevent AS clause
    return is_pg_ ? error::IllegalChangeStreamQueryPGSyntax(tvf_name_)
                  : error::IllegalChangeStreamQuerySyntax(tvf_name_);
  }
  std::vector<googlesql::Value> arguments;
  arguments.reserve(tvf_scan->argument_list_size());
  for (int i = 0; i < tvf_scan->argument_list_size(); i++) {
    const googlesql::ResolvedFunctionArgument* arg = tvf_scan->argument_list(i);
    const googlesql::ResolvedExpr* arg_expr = arg->expr();
    if (arg_expr->Is<googlesql::ResolvedLiteral>()) {
      arguments.push_back(
          arg_expr->GetAs<googlesql::ResolvedLiteral>()->value());
    } else if (arg_expr->Is<googlesql::ResolvedParameter>()) {
      arguments.push_back(
          params_.at(arg_expr->GetAs<googlesql::ResolvedParameter>()->name()));
    } else {  // prevent scalar argument
      return error::InvalidChangeStreamTvfArgumentWithArgIndex(tvf_name_, i);
    }
  }
  // Validation of partition token string will happen in later querying stage
  GOOGLESQL_RETURN_IF_ERROR(ValidateTimeStamps(arguments[0], arguments[1]));
  GOOGLESQL_RETURN_IF_ERROR(ValidateHeartbeatMilliseconds(arguments[3]));
  GOOGLESQL_RETURN_IF_ERROR(ValidateReadOptions(arguments[4]));

  change_stream_metadata_ =
      ChangeStreamMetadata{tvf_name_, arguments, is_pg_, schema_};
  return absl::OkStatus();
}

// Validate the argument values and query syntax, query syntax must be in the
// form of query_stmt->project_scan->tvf_scan
absl::Status ChangeStreamQueryValidator::ValidateQuery(
    const googlesql::ResolvedNode* node) {
  ABSL_DCHECK(node != nullptr);
  if (node->node_kind() == googlesql::RESOLVED_QUERY_STMT) {
    const googlesql::ResolvedQueryStmt* query_stmt =
        node->GetAs<googlesql::ResolvedQueryStmt>();
    // prevent multiple output columns or single column other than
    // "ChangeRecord" in googlesql or the corresponding tvf name "read_json_xxx"
    // in postgres.
    if (query_stmt->query()->node_kind() != googlesql::RESOLVED_PROJECT_SCAN ||
        query_stmt->output_column_list().size() != 1 ||
        query_stmt->output_column_list().at(0)->name() !=
            (is_pg_ ? tvf_name_ : kChangeStreamTvfOutputColumn)) {
      return is_pg_ ? error::IllegalChangeStreamQueryPGSyntax(tvf_name_)
                    : error::IllegalChangeStreamQuerySyntax(tvf_name_);
    }
  }
  // prevent other scans except for RESOLVED_TVF_SCAN
  if (node->node_kind() == googlesql::RESOLVED_PROJECT_SCAN) {
    if (node->GetAs<googlesql::ResolvedProjectScan>()
            ->input_scan()
            ->node_kind() != googlesql::RESOLVED_TVFSCAN) {
      return is_pg_ ? error::IllegalChangeStreamQueryPGSyntax(tvf_name_)
                    : error::IllegalChangeStreamQuerySyntax(tvf_name_);
    }
  }
  if (node->node_kind() == googlesql::RESOLVED_TVFSCAN) {
    const googlesql::ResolvedTVFScan* tvf_scan =
        node->GetAs<googlesql::ResolvedTVFScan>();
    GOOGLESQL_RETURN_IF_ERROR(ValidateTvfScanAndExtractMetadata(tvf_scan));
  }
  std::vector<const googlesql::ResolvedNode*> child_nodes;
  node->GetChildNodes(&child_nodes);
  for (const googlesql::ResolvedNode* child : child_nodes) {
    GOOGLESQL_RETURN_IF_ERROR(ChangeStreamQueryValidator::ValidateQuery(child));
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> ChangeStreamQueryValidator::IsChangeStreamQuery(
    const googlesql::ResolvedNode* node) {
  ABSL_DCHECK(node != nullptr);
  std::vector<const googlesql::ResolvedNode*> child_nodes;
  node->GetChildNodes(&child_nodes);
  for (const googlesql::ResolvedNode* child : child_nodes) {
    GOOGLESQL_ASSIGN_OR_RETURN(auto is_change_stream, IsChangeStreamQuery(child));
    if (is_change_stream) {
      return true;
    }
  }
  if (node->node_kind() == googlesql::RESOLVED_TVFSCAN) {
    absl::string_view tvf_name =
        node->GetAs<googlesql::ResolvedTVFScan>()->tvf()->Name();
    bool has_valid_prefix = false;
    if (is_pg_) {
      if (absl::ConsumePrefix(&tvf_name, kChangeStreamTvfJsonPrefix) ||
          absl::ConsumePrefix(&tvf_name, kChangeStreamTvfProtoBytesPrefix)) {
        has_valid_prefix = true;
      }
    } else {
      if (absl::ConsumePrefix(&tvf_name, kChangeStreamTvfStructPrefix)) {
        has_valid_prefix = true;
      }
    }
    if (has_valid_prefix &&
        schema_->FindChangeStream(std::string(tvf_name)) != nullptr) {
      change_stream_name_ = std::string(tvf_name);
      tvf_name_ = node->GetAs<googlesql::ResolvedTVFScan>()->tvf()->Name();
      return true;
    }
  }
  change_stream_metadata_ = ChangeStreamMetadata{};
  return false;
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
