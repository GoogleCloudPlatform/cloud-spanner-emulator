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

#include "backend/actions/change_stream.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// TODO: Since the partitions don't change within a transaction,
// reading partition token only once for each transaction after GetBufferedOps
// in the transaction store.
absl::StatusOr<zetasql::Value> GetPartitionToken(
    const ActionContext* ctx, const ChangeStream* change_stream) {
  // Get partition_token to make the primary key for change stream data
  // table
  std::vector<const Column*> read_columns = {
      change_stream->change_stream_partition_table()->FindColumn(
          "partition_token"),
      change_stream->change_stream_partition_table()->FindColumn("end_time")};
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(change_stream->change_stream_partition_table(),
                         KeyRange::All(), read_columns));
  std::vector<std::string> active_partition_tokens;
  while (itr->Next()) {
    // Find active partitions by filtering partitions with the end_time equal
    // to the default null end_timestamp. Stale partitions' end_timestamps are
    // set to the transaction commit timestamp.
    if (itr->ColumnValue(1).is_null()) {
      active_partition_tokens.push_back(itr->ColumnValue(0).string_value());
    }
  }
  std::sort(active_partition_tokens.begin(), active_partition_tokens.end());
  ZETASQL_RET_CHECK(!active_partition_tokens.empty());
  return zetasql::Value::String(active_partition_tokens[0]);
}
}  // namespace

ChangeStreamEffector::ChangeStreamEffector(const ChangeStream* change_stream)
    : change_stream_(change_stream) {}

absl::Status ChangeStreamEffector::Effect(const ActionContext* ctx,
                                          const InsertOp& op) const {
  ZETASQL_ASSIGN_OR_RETURN(zetasql::Value partition_token,
                   GetPartitionToken(ctx, change_stream_));
  // WriteOps will be grouped in ChangeStreamEffectsBuffer to make
  // DataChangeRecords. Pass the partition token of the partition the op will be
  // written to, the change stream tracking the table op belonging to, and the
  // op itself to ChangeStreamEffectsBuffer.
  ctx->change_stream_effects()->Insert(partition_token, change_stream_, op);

  return absl::OkStatus();
}

absl::Status ChangeStreamEffector::Effect(const ActionContext* ctx,
                                          const UpdateOp& op) const {
  ZETASQL_ASSIGN_OR_RETURN(zetasql::Value partition_token,
                   GetPartitionToken(ctx, change_stream_));
  // Insert the new row in the change_stream_data_table.
  ctx->change_stream_effects()->Update(partition_token, change_stream_, op);
  return absl::OkStatus();
}

absl::Status ChangeStreamEffector::Effect(const ActionContext* ctx,
                                          const DeleteOp& op) const {
  ZETASQL_ASSIGN_OR_RETURN(zetasql::Value partition_token,
                   GetPartitionToken(ctx, change_stream_));
  // Insert the new row in the change_stream_data_table.
  ctx->change_stream_effects()->Delete(partition_token, change_stream_, op);
  return absl::OkStatus();
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
