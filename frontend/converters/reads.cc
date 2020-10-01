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

#include "frontend/converters/reads.h"

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/keys.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "backend/access/write.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/transaction/options.h"
#include "common/errors.h"
#include "common/limits.h"
#include "frontend/converters/chunking.h"
#include "frontend/converters/keys.h"
#include "frontend/converters/partition.h"
#include "frontend/converters/time.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "frontend/proto/partition_token.pb.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

absl::Status ResultSetMetadataToProto(backend::RowCursor* cursor,
                                      v1::ResultSetMetadata* metadata_pb) {
  for (int i = 0; i < cursor->NumColumns(); ++i) {
    auto* field_pb = metadata_pb->mutable_row_type()->add_fields();
    field_pb->set_name(cursor->ColumnName(i));
    ZETASQL_RETURN_IF_ERROR(
        TypeToProto(cursor->ColumnType(i), field_pb->mutable_type()))
        << " when converting column " << cursor->ColumnName(i) << " of type "
        << cursor->ColumnType(i) << " at position " << i << " in row cursor";
  }
  return absl::OkStatus();
}

absl::Status ValidateStaleness(absl::Duration staleness) {
  if (staleness < absl::ZeroDuration()) {
    return error::StalenessMustBeNonNegative();
  }
  return absl::OkStatus();
}

absl::Status ValidateMinReadTimestamp(absl::Time min_read_timestamp) {
  const int64_t timestamp = absl::ToUnixMicros(min_read_timestamp);
  if (timestamp < 0 || timestamp == std::numeric_limits<int64_t>::max()) {
    return error::InvalidMinReadTimestamp(min_read_timestamp);
  }
  return absl::OkStatus();
}

absl::Status ValidateExactReadTimestamp(absl::Time exact_read_timestamp) {
  const int64_t timestamp = absl::ToUnixMicros(exact_read_timestamp);
  if (timestamp < 0 || timestamp == std::numeric_limits<int64_t>::max()) {
    return error::InvalidExactReadTimestamp(exact_read_timestamp);
  }
  return absl::OkStatus();
}

absl::Status ValidatePartitionToken(
    const PartitionToken& partition_token,
    const google::spanner::v1::ReadRequest& request) {
  if (partition_token.session() != request.session()) {
    return error::ReadFromDifferentSession();
  }

  if (request.transaction().selector_case() != v1::TransactionSelector::kId ||
      partition_token.transaction_id() != request.transaction().id()) {
    return error::ReadFromDifferentTransaction();
  }

  if (!partition_token.has_read_params()) {
    return error::ReadFromDifferentParameters();
  }
  auto read_params = partition_token.read_params();

  if (read_params.table() != request.table() ||
      read_params.index() != request.index() ||
      read_params.columns_size() != request.columns_size()) {
    return error::ReadFromDifferentParameters();
  }

  for (int i = 0; i < request.columns_size(); i++) {
    if (read_params.columns(i) != request.columns(i)) {
      return error::ReadFromDifferentParameters();
    }
  }

  if (read_params.key_set().SerializeAsString() !=
      request.key_set().SerializeAsString()) {
    return error::ReadFromDifferentParameters();
  }

  return absl::OkStatus();
}

}  // namespace

zetasql_base::StatusOr<backend::ReadOnlyOptions> ReadOnlyOptionsFromProto(
    const spanner_api::TransactionOptions::ReadOnly& proto) {
  using ReadOnly = spanner_api::TransactionOptions::ReadOnly;
  backend::ReadOnlyOptions options;
  switch (proto.timestamp_bound_case()) {
    case ReadOnly::kMinReadTimestamp: {
      ZETASQL_ASSIGN_OR_RETURN(options.timestamp,
                       TimestampFromProto(proto.min_read_timestamp()));
      ZETASQL_RETURN_IF_ERROR(ValidateMinReadTimestamp(options.timestamp));
      options.bound = backend::TimestampBound::kMinTimestamp;
      break;
    }
    case ReadOnly::kMaxStaleness: {
      ZETASQL_ASSIGN_OR_RETURN(options.staleness,
                       DurationFromProto(proto.max_staleness()));
      ZETASQL_RETURN_IF_ERROR(ValidateStaleness(options.staleness));
      options.bound = backend::TimestampBound::kMaxStaleness;
      break;
    }
    case ReadOnly::kReadTimestamp: {
      ZETASQL_ASSIGN_OR_RETURN(options.timestamp,
                       TimestampFromProto(proto.read_timestamp()));
      ZETASQL_RETURN_IF_ERROR(ValidateExactReadTimestamp(options.timestamp));
      options.bound = backend::TimestampBound::kExactTimestamp;
      break;
    }
    case ReadOnly::kExactStaleness: {
      ZETASQL_ASSIGN_OR_RETURN(options.staleness,
                       DurationFromProto(proto.exact_staleness()));
      ZETASQL_RETURN_IF_ERROR(ValidateStaleness(options.staleness));
      options.bound = backend::TimestampBound::kExactStaleness;
      break;
    }
    case ReadOnly::kStrong:
      if (!proto.strong()) {
        return error::StrongReadOptionShouldBeTrue();
      }
      ABSL_FALLTHROUGH_INTENDED;
    case ReadOnly::TIMESTAMP_BOUND_NOT_SET:
      options.bound = backend::TimestampBound::kStrongRead;
      break;
  }
  return options;
}

absl::Status ReadArgFromProto(const backend::Schema& schema,
                              const google::spanner::v1::ReadRequest& request,
                              backend::ReadArg* read_arg) {
  if (!request.has_key_set()) {
    return error::MissingRequiredFieldError("ReadRequest.key_set");
  }
  if (request.table().empty()) {
    return error::MutationTableRequired();
  }
  if (request.limit() < 0) {
    return error::InvalidReadLimit();
  }
  if (request.limit() > 0 && !request.partition_token().empty()) {
    return error::InvalidReadLimitWithPartitionToken();
  }

  auto key_set = request.key_set();
  if (!request.partition_token().empty()) {
    ZETASQL_ASSIGN_OR_RETURN(auto partition_token,
                     PartitionTokenFromString(request.partition_token()));
    ZETASQL_RETURN_IF_ERROR(ValidatePartitionToken(partition_token, request));
    key_set = partition_token.partitioned_key_set();
  }

  read_arg->table = request.table();
  read_arg->index = request.index();
  read_arg->columns.assign(request.columns().begin(), request.columns().end());

  const backend::Table* table = schema.FindTable(request.table());
  if (table == nullptr) {
    return error::TableNotFound(request.table());
  }
  if (!request.index().empty()) {
    const backend::Index* index = schema.FindIndex(request.index());
    if (index == nullptr) {
      return error::IndexNotFound(request.index(), request.table());
    }
    table = index->index_data_table();
  }

  ZETASQL_ASSIGN_OR_RETURN(read_arg->key_set, KeySetFromProto(key_set, *table));
  return absl::OkStatus();
}

absl::Status RowCursorToResultSetProto(backend::RowCursor* cursor, int limit,
                                       spanner_api::ResultSet* result_pb) {
  ZETASQL_RETURN_IF_ERROR(
      ResultSetMetadataToProto(cursor, result_pb->mutable_metadata()));

  // Iterate over all rows and populate column values into ResultSet.
  int row_count = 0;
  while (cursor->Next()) {
    auto* row_pb = result_pb->add_rows();
    for (int i = 0; i < cursor->NumColumns(); ++i) {
      ZETASQL_ASSIGN_OR_RETURN(*row_pb->add_values(),
                       ValueToProto(cursor->ColumnValue(i)));
    }
    ++row_count;
    if (limit > 0 && limit == row_count) {
      break;
    }
  }

  return absl::OkStatus();
}

zetasql_base::StatusOr<std::vector<spanner_api::PartialResultSet>>
RowCursorToPartialResultSetProtos(backend::RowCursor* cursor, int limit) {
  spanner_api::ResultSet result_set;
  ZETASQL_RETURN_IF_ERROR(RowCursorToResultSetProto(cursor, limit, &result_set));
  return ChunkResultSet(result_set, limits::kMaxStreamingChunkSize);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
