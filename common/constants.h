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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CONSTANTS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CONSTANTS_H_

#include <cstdint>
#include <limits>

#include "zetasql/public/type.h"

constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

// Name of the function to write the commit timestamp in a DML statement. Cloud
// Spanner selects the commit timestamp when the transaction commits.
constexpr char kPendingCommitTimestampFunctionName[] =
    "pending_commit_timestamp";

// Name of the bit_reverse function.
constexpr char kBitReverseFunctionName[] = "bit_reverse";

// Name of the get_internal_sequence_state function.
constexpr char kGetInternalSequenceStateFunctionName[] =
    "get_internal_sequence_state";

constexpr char kGetNextSequenceValueFunctionName[] = "get_next_sequence_value";

// Name of the ml_predict_row function.
constexpr char kMlPredictRowFunctionName[] = "ml_predict_row";

// String used to tell cloud spanner to insert the commit timestamp into a
// TIMESTAMP column with allow_commit_timestamp option set to true upon
// transaction commit.
constexpr char kCommitTimestampIdentifier[] = "spanner.commit_timestamp()";

// Max googlesql timestamp value is used as a sentinel by transaction store to
// identify if client requested commit timestamp to be read/inserted. At flush,
// this sentinel value is replaced by actual transaction commit timestamp.
//
// Note that for a non-commit timestamp column, this is a valid column value to
// be passed by a client directly and won't be replaced by commit timestamp.
//
// Whereas, a timestamp column with allow_commit_timestamp set to true can't
// have a timestamp in future and thus this sentinel value is not a valid value
// for the column to be passed by a client.
constexpr absl::Time kCommitTimestampValueSentinel =
    absl::FromUnixMicros(zetasql::types::kTimestampMax);

// gRPC ResourceInfo binary metadata header.
constexpr char kResourceInfoBinaryHeader[] = "google.rpc.resourceinfo-bin";

// ResourceInfo URL used for including metadata in gRPC error details.
constexpr char kResourceInfoType[] =
    "type.googleapis.com/google.rpc.ResourceInfo";

// ConstraintError URL used to mark if a transaction encountered a constraint
// error. This does not correspond with an actual proto.
constexpr char kConstraintError[] = "google.spanner.ConstraintError";

// Session resource type.
constexpr char kSessionResourceType[] =
    "type.googleapis.com/google.spanner.v1.Session";

// Database resource type.
constexpr char kDatabaseResourceType[] =
    "type.googleapis.com/google.spanner.admin.database.v1.Database";

// Instance resource type.
constexpr char kInstanceResourceType[] =
    "type.googleapis.com/google.spanner.admin.instance.v1.Instance";

// The default timezone used by the query engine.
constexpr char kDefaultTimeZone[] = "America/Los_Angeles";

// Change stream tvf output column name and type
constexpr char kChangeStreamTvfOutputColumn[] = "ChangeRecord";
constexpr char kChangeStreamTvfOutputFormat[] = R"(ARRAY<STRUCT<
  data_change_record ARRAY<STRUCT<
    commit_timestamp TIMESTAMP,
    record_sequence STRING,
    server_transaction_id STRING,
    is_last_record_in_transaction_in_partition BOOL,
    table_name STRING,
    column_types ARRAY<STRUCT<
      name STRING,
      type $0,
      is_primary_key BOOL,
      ordinal_position INT64>>,
    mods ARRAY<STRUCT<
      keys $0,
      new_values $0,
      old_values $0>>,
    mod_type STRING,
    value_capture_type STRING,
    number_of_records_in_transaction INT64,
    number_of_partitions_in_transaction INT64,
    transaction_tag STRING,
    is_system_transaction BOOL>>,
  heartbeat_record ARRAY<STRUCT<
    timestamp TIMESTAMP>>,
  child_partitions_record ARRAY<STRUCT<
    start_timestamp TIMESTAMP,
    record_sequence STRING,
    child_partitions ARRAY<STRUCT<
      token STRING,
      parent_partition_tokens ARRAY<STRING>>>>>>>)";
// Prefix for change stream tvf in googlesql dialect
constexpr char kChangeStreamTvfStructPrefix[] = "READ_";
// Prefix for change stream tvf in postgres dialect
constexpr char kChangeStreamTvfJsonPrefix[] = "read_json_";
// Prefix for change stream data table
static constexpr char kChangeStreamDataTablePrefix[] = "_change_stream_data_";
// Prefix for change stream partition table
static constexpr char kChangeStreamPartitionTablePrefix[] =
    "_change_stream_partition_";

// Options for bit-reversed sequences.
static constexpr char kSequenceKindOptionName[] = "sequence_kind";
static constexpr char kSequenceKindBitReversedPositive[] =
    "bit_reversed_positive";
static constexpr char kSequenceSkipRangeMinOptionName[] = "skip_range_min";
static constexpr char kSequenceSkipRangeMaxOptionName[] = "skip_range_max";
static constexpr char kSequenceStartWithCounterOptionName[] =
    "start_with_counter";

// Default start_with value for a sequence, when the sequence does not
// have a user-set start_with.
constexpr int64_t kSequenceDefaultStartWith = 1;

// Quotes used by different dialects of Spanner in DDL statements.
static constexpr char kGSQLQuote[] = "`";
static constexpr char kPGQuote[] = "\"";
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CONSTANTS_H_
