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

#include "backend/schema/backfills/change_stream_backfill.h"

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/strings/escaping.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/common/rows.h"
#include "backend/schema/catalog/column.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::StatusOr<Key> ComputeChangeStreamPartitionTableKey(
    std::string partition_token_str, const ChangeStream* change_stream) {
  // Columns must be added to the key for each column in change stream partition
  // table primary key.
  Key key;
  key.AddColumn(zetasql::Value::String(partition_token_str),
                change_stream->change_stream_partition_table()
                    ->FindKeyColumn("partition_token")
                    ->is_descending());
  const int64_t key_size = key.LogicalSizeInBytes();
  if (key_size > limits::kMaxKeySizeBytes) {
    return error::KeyTooLarge(
        change_stream->change_stream_partition_table()->Name(), key_size,
        limits::kMaxKeySizeBytes);
  }
  return key;
}

std::vector<zetasql::Value> CreateInitialBackfillPartitions(
    std::vector<zetasql::Value> row_values, std::string partition_token_str,
    absl::Time start_time, std::string churn_type) {
  // specify partition_token
  row_values.push_back(zetasql::Value::String(partition_token_str));
  // Specify start_time
  row_values.push_back(zetasql::Value::Timestamp(start_time));
  // Specify end_time
  row_values.push_back(zetasql::Value::NullTimestamp());
  // Specify parents
  row_values.push_back(
      zetasql::Value::EmptyArray(zetasql::types::StringArrayType()));
  // Specify children
  row_values.push_back(
      zetasql::Value::EmptyArray(zetasql::types::StringArrayType()));
  // Specify the churn type.
  row_values.push_back(zetasql::Value::String(churn_type));
  return row_values;
}

std::string gen_random(const int len) {
  std::srand(absl::ToUnixMicros(absl::Now()));
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s, token_string;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  token_string = absl::WebSafeBase64Escape(tmp_s);
  return token_string;
}

std::string CreatePartitionTokenString() {
  const int64_t min_partition_token_len = 130;
  const int64_t max_partition_token_len = 170;
  const int64_t partition_token_len =
      min_partition_token_len +
      rand() % (max_partition_token_len - min_partition_token_len) + 1;
  return gen_random(partition_token_len);
}

absl::Status BackfillChangeStreamPartition(
    const SchemaValidationContext* context, const ChangeStream* change_stream,
    absl::Span<const Column* const> change_stream_partition_table_columns,
    std::vector<ColumnID> change_stream_partition_table_column_ids,
    std::string churn_type) {
  std::string partition_token_str = CreatePartitionTokenString();
  // Populate values for the row representing the first partition
  std::vector<zetasql::Value> initial_row_values;
  initial_row_values.reserve(change_stream_partition_table_columns.size());
  initial_row_values = CreateInitialBackfillPartitions(
      initial_row_values, partition_token_str, change_stream->creation_time(),
      churn_type);
  // Create Key for the change stream partition table
  ZETASQL_ASSIGN_OR_RETURN(
      Key change_stream_partition_table_key,
      ComputeChangeStreamPartitionTableKey(partition_token_str, change_stream),
      _.SetErrorCode(absl::StatusCode::kFailedPrecondition));
  // Insert the second new row in the change_stream_partition_table.
  ZETASQL_RETURN_IF_ERROR(context->storage()->Write(
      context->pending_commit_timestamp(),
      change_stream->change_stream_partition_table()->id(),
      change_stream_partition_table_key,
      change_stream_partition_table_column_ids, initial_row_values));
  return absl::OkStatus();
}

absl::Status BackfillChangeStream(const ChangeStream* change_stream,
                                  const SchemaValidationContext* context) {
  const Table* change_stream_partition_table =
      change_stream->change_stream_partition_table();
  absl::Span<const Column* const> change_stream_partition_table_columns =
      change_stream_partition_table->columns();
  std::vector<ColumnID> change_stream_partition_table_column_ids =
      GetColumnIDs(change_stream_partition_table_columns);
  // Backfill the first partition
  // Generate partition token
  const int64_t num_initial_partitions = 2;
  for (int i = 0; i < num_initial_partitions; ++i) {
    std::string churn_type = "MOVE";
    if (i == 0) {
      // The first initial token will be continually merging and splitting.
      // The second token will continually be moving (i.e. one parent token
      // generates one child token.)
      churn_type = "SPLIT";
    }
    ZETASQL_RETURN_IF_ERROR(BackfillChangeStreamPartition(
        context, change_stream, change_stream_partition_table_columns,
        change_stream_partition_table_column_ids, churn_type));
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
