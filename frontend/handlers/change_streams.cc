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

#include "frontend/handlers/change_streams.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/query/change_stream/change_stream_query_validator.h"
#include "backend/query/query_engine.h"
#include "backend/schema/catalog/schema.h"
#include "common/clock.h"
#include "common/errors.h"
#include "frontend/converters/change_streams.h"
#include "frontend/converters/time.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
#include "frontend/server/handler.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(bool, cloud_spanner_emulator_test_with_fake_partition_table, false,
          "TEST ONLY. Set to true to enable querying against mocked change "
          "stream internal partition table during test.");

ABSL_FLAG(
    absl::Duration, change_streams_partition_query_chop_interval,
    absl::Milliseconds(100),
    "Change streams chopped interval for partition query in milliseconds.");

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {
// We only allow single-use read-only strong transaction for change stream
// queries
absl::Status ValidateTransactionSelectorForChangeStreamQuery(
    const spanner_api::TransactionSelector& selector) {
  // Default transaction selector is single use read only strong transaction, so
  // directly returns ok
  if (selector.selector_case() ==
      spanner_api::TransactionSelector::SELECTOR_NOT_SET) {
    return absl::OkStatus();
  }
  if (selector.selector_case() !=
      spanner_api::TransactionSelector::SelectorCase::kSingleUse) {
    return error::ChangeStreamQueriesMustBeSingleUseOnly();
  }
  if (!selector.single_use().read_only().has_strong()) {
    return error::ChangeStreamQueriesMustBeStrongReads();
  }
  return absl::OkStatus();
}

absl::Status VerifyChangeStreamExistence(const std::string& change_stream_name,
                                         const backend::Schema* schema) {
  auto change_stream = schema->FindChangeStream(change_stream_name);
  if (change_stream != nullptr) {
    return absl::OkStatus();
  }
  return error::ChangeStreamNotFound(change_stream_name);
}

absl::StatusOr<absl::Duration> TryGetChangeStreamRetentionPeriod(
    const std::string& change_stream_name, std::shared_ptr<Session> session,
    absl::Time read_ts) {
  spanner_api::TransactionOptions txn_options;
  txn_options.mutable_read_only()->set_return_read_timestamp(false);
  // If the user provided tvf start time is past now, wait until this future
  // time to perform a read on partition token end time.
  ZETASQL_ASSIGN_OR_RETURN(
      *txn_options.mutable_read_only()->mutable_min_read_timestamp(),
      TimestampToProto(read_ts));
  ZETASQL_ASSIGN_OR_RETURN(auto txn, session->CreateSingleUseTransaction(txn_options));
  auto change_stream = txn->schema()->FindChangeStream(change_stream_name);
  if (change_stream != nullptr) {
    return absl::Seconds(change_stream->parsed_retention_period());
  }
  return error::ChangeStreamNotFound(change_stream_name);
}

bool IsQueryResultEmpty(backend::QueryResult& result) {
  return result.num_output_rows == 0;
}

absl::Status ValidateTokenInRetentionWindow(
    const absl::Time tvf_start, const absl::Time current_chopped_start,
    const absl::Time current_token_end,
    const absl::Duration current_retention) {
  absl::Time gc_time = Clock().Now() - current_retention;
  // If current chopped start is before gc_time, we know this token must be
  // invalid and need to identify the correct error type.
  if (current_chopped_start < gc_time) {
    // If current token already end before gc_time, this entire partition has
    // expired.
    if (current_token_end < gc_time) {
      return error::ChangeStreamStalePartition();
    }
    // Although token is not expired, the user provided tvf start time is too
    // old.
    return error::InvalidChangeStreamTvfArgumentStartTimestampTooOld(
        absl::FormatTime(gc_time), absl::FormatTime(tvf_start));
  }
  return absl::OkStatus();
}

absl::Status ProcessDataChangeRecordsAndStreamBack(
    backend::QueryResult& result, const bool expect_heartbeat,
    const absl::Time scan_end, bool* expect_metadata,
    absl::Time* last_record_time,
    ServerStream<spanner_api::PartialResultSet>* stream) {
  std::vector<spanner_api::PartialResultSet> responses;
  if (IsQueryResultEmpty(result) && expect_heartbeat) {
    ZETASQL_ASSIGN_OR_RETURN(responses, ConvertHeartbeatTimestampToStruct(
                                    scan_end, *expect_metadata));
    *expect_metadata = false;
    *last_record_time = scan_end;
  } else if (!IsQueryResultEmpty(result)) {
    ZETASQL_ASSIGN_OR_RETURN(responses, ConvertDataTableRowCursorToStruct(
                                    result.rows.get(), *expect_metadata));
    *last_record_time = scan_end;
    *expect_metadata = false;
  }
  for (auto& response : responses) {
    stream->Send(response);
  }
  return absl::OkStatus();
}
}  // namespace

absl::Status ChangeStreamsHandler::ExecuteInitialQuery(
    std::shared_ptr<Session> session,
    ServerStream<spanner_api::PartialResultSet>* stream) {
  spanner_api::TransactionOptions txn_options;
  ZETASQL_ASSIGN_OR_RETURN(
      *txn_options.mutable_read_only()->mutable_min_read_timestamp(),
      TimestampToProto(metadata().start_timestamp));
  txn_options.mutable_read_only()->set_return_read_timestamp(false);
  ZETASQL_ASSIGN_OR_RETURN(auto txn, session->CreateSingleUseTransaction(txn_options));
  return txn->GuardedCall(Transaction::OpType::kSql, [&]() -> absl::Status {
    ZETASQL_RETURN_IF_ERROR(VerifyChangeStreamExistence(metadata().change_stream_name,
                                                txn->schema()));
    backend::Query initial_query = backend::Query{absl::Substitute(
        "SELECT start_time, partition_token, parents "
        "FROM $0 "
        "WHERE '$1' >= start_time AND ( end_time IS NULL OR '$1' < end_time "
        ")  ORDER BY (partition_token)",
        partition_table_, metadata().start_timestamp)};
    initial_query.change_stream_internal_lookup = metadata().change_stream_name;
    ZETASQL_ASSIGN_OR_RETURN(auto partition_results, txn->ExecuteSql(initial_query));
    // Initial query is guaranteed to return at least 1 child partition record.
    ZETASQL_RET_CHECK(!IsQueryResultEmpty(partition_results));

    ZETASQL_ASSIGN_OR_RETURN(auto responses, ConvertPartitionTableRowCursorToStruct(
                                         partition_results.rows.get(),
                                         metadata().start_timestamp,
                                         /*need_metadata=*/true));

    for (auto& response : responses) {
      stream->Send(response);
    }
    return absl::OkStatus();
  });
}

absl::StatusOr<absl::Time> ChangeStreamsHandler::TryGetPartitionTokenEndTime(
    std::shared_ptr<Session> session, absl::Time read_ts) const {
  absl::Time start, end;
  spanner_api::TransactionOptions txn_options;
  txn_options.mutable_read_only()->set_return_read_timestamp(false);
  // If the user provided tvf start time is past now, wait until this future
  // time to perform a read on partition token end time.
  ZETASQL_ASSIGN_OR_RETURN(*txn_options.mutable_read_only()->mutable_read_timestamp(),
                   TimestampToProto(read_ts));
  ZETASQL_ASSIGN_OR_RETURN(auto txn, session->CreateSingleUseTransaction(txn_options));
  ZETASQL_RETURN_IF_ERROR(
      txn->GuardedCall(Transaction::OpType::kSql, [&]() -> absl::Status {
        backend::Query get_partition_token_time_query =
            backend::Query{absl::Substitute(
                "SELECT start_time, end_time "
                "FROM $0 "
                "WHERE( partition_token = '$1' )",
                partition_table_, metadata().partition_token.value())};
        get_partition_token_time_query.change_stream_internal_lookup =
            metadata().change_stream_name;
        ZETASQL_ASSIGN_OR_RETURN(auto token_time_results,
                         txn->ExecuteSql(get_partition_token_time_query));
        if (IsQueryResultEmpty(token_time_results)) {
          return error::
              InvalidChangeStreamTvfArgumentPartitionTokenInvalidChangeStreamName(  // NOLINT
                  metadata().partition_token.value());
        }
        backend::RowCursor* cursor = token_time_results.rows.get();
        cursor->Next();
        start = cursor->ColumnValue(0).ToTime();
        // If the end_time of current partition token is null, set the returning
        // end time to InfiniteFuture().
        end = cursor->ColumnValue(1).is_null()
                  ? absl::InfiniteFuture()
                  : cursor->ColumnValue(1).ToTime();
        // Return error if user provided tvf start time is not within the
        // lifetime of the user provided partition token.
        if (metadata().start_timestamp < start ||
            metadata().start_timestamp > end) {
          return error::
              InvalidChangeStreamTvfArgumentStartTimestampForPartition(
                  absl::FormatTime(start), absl::FormatTime(end),
                  absl::FormatTime(metadata().start_timestamp));
        }
        return absl::OkStatus();
      }));
  return end;
}

backend::Query ChangeStreamsHandler::ConstructDataTablePartitionQuery(
    absl::Time start, absl::Time end) const {
  // If user passed end_timestamp is not null and current scan is the last scan
  // in query lifetime, we do an inclusive scan to include the data change
  // record with commit_timestamp exactly at the user passed end_timestamp. If
  // current scan is a middle chopped scan, we do an exclusive scan because all
  // data records of a partition token has a commit_timestamp in
  // [partition_start_time,partition_end_time).
  const bool is_inclusive_read = metadata().end_timestamp.has_value() &&
                                 metadata().end_timestamp.value() == end;
  backend::Query data_table_partition_query = backend::Query{absl::Substitute(
      "SELECT * "
      "FROM $0 "
      "WHERE( partition_token='$1' AND commit_timestamp >= '$2' AND "
      "commit_timestamp $3 '$4' ) ORDER BY partition_token, commit_timestamp, "
      "server_transaction_id,record_sequence",
      metadata().data_table, metadata().partition_token.value(), start,
      is_inclusive_read ? "<=" : "<", end)};
  data_table_partition_query.change_stream_internal_lookup =
      metadata().change_stream_name;
  return data_table_partition_query;
}

backend::Query ChangeStreamsHandler::ConstructPartitionTablePartitionQuery()
    const {
  backend::Query data_table_partition_query = backend::Query{absl::Substitute(
      "SELECT start_time, partition_token, parents FROM $0 "
      "WHERE (ARRAY_INCLUDES((SELECT children FROM $0 WHERE "
      "partition_token = '$1'), partition_token)) ORDER BY(partition_token)",
      partition_table_, metadata().partition_token.value())};
  data_table_partition_query.change_stream_internal_lookup =
      metadata().change_stream_name;
  return data_table_partition_query;
}

absl::Status ChangeStreamsHandler::ExecutePartitionQuery(
    ServerStream<spanner_api::PartialResultSet>* stream,
    std::shared_ptr<Session> session) {
  const absl::Time tvf_end = metadata().end_timestamp.has_value()
                                 ? metadata().end_timestamp.value()
                                 : absl::InfiniteFuture();
  const absl::Time now = Clock().Now();
  const absl::Duration heartbeat_interval =
      absl::Milliseconds(metadata().heartbeat_milliseconds);
  absl::Time last_record_time = now;
  absl::Time partition_token_end_time = absl::InfiniteFuture();
  absl::Time current_start = metadata().start_timestamp;
  absl::Time current_end = std::min(
      std::max(now,
               current_start +
                   absl::GetFlag(
                       FLAGS_change_streams_partition_query_chop_interval)),
      tvf_end);
  // Metadata is only expected for the first response to users in a single
  // query's lifetime.
  bool expect_metadata = true;
  while (current_start <= tvf_end && current_start < partition_token_end_time) {
    // For historical queries where tvf end is in the past, set the read
    // transaction snapshot time to now to prevent >1h stale read, which is now
    // allowed.
    absl::Time current_txn_snapshot_time = std::max(current_end, now);
    // Get the newest retention period so most up to date retention will apply
    // to curent running query.
    ZETASQL_ASSIGN_OR_RETURN(
        absl::Duration current_retention,
        TryGetChangeStreamRetentionPeriod(metadata().change_stream_name,
                                          session, current_txn_snapshot_time));
    spanner_api::TransactionOptions txn_options;
    // If the partition token hasn't been churned yet, we re-scan the partition
    // table to see if the end time has been churned and update the partition
    // end time.
    if (partition_token_end_time == absl::InfiniteFuture()) {
      ZETASQL_ASSIGN_OR_RETURN(
          partition_token_end_time,
          TryGetPartitionTokenEndTime(session, current_txn_snapshot_time));
    }
    ZETASQL_RETURN_IF_ERROR(ValidateTokenInRetentionWindow(
        metadata().start_timestamp, current_start, partition_token_end_time,
        current_retention));
    // Only scan data records up to minimum of current chopped end time
    // and end time of current partition token.
    const absl::Time scan_end = std::min(partition_token_end_time, current_end);
    const bool expect_heartbeat =
        current_end - last_record_time >= heartbeat_interval;
    // This transaction will be blocked until now passes current_end.
    ZETASQL_ASSIGN_OR_RETURN(*txn_options.mutable_read_only()->mutable_read_timestamp(),
                     TimestampToProto(current_txn_snapshot_time));
    ZETASQL_ASSIGN_OR_RETURN(auto txn,
                     session->CreateSingleUseTransaction(txn_options));
    absl::Status status =
        txn->GuardedCall(Transaction::OpType::kSql, [&]() -> absl::Status {
          backend::Query read_data_query =
              ConstructDataTablePartitionQuery(current_start, scan_end);
          ZETASQL_ASSIGN_OR_RETURN(auto data_records_results,
                           txn->ExecuteSql(read_data_query));
          ZETASQL_RETURN_IF_ERROR(ProcessDataChangeRecordsAndStreamBack(
              data_records_results, expect_heartbeat, scan_end,
              &expect_metadata, &last_record_time, stream));
          if (partition_token_end_time <= current_end) {
            // Get child partition records after all data records are returned
            // in current query.
            backend::Query tail_query_partition_table =
                ConstructPartitionTablePartitionQuery();
            ZETASQL_ASSIGN_OR_RETURN(auto tail_partition_records_results,
                             txn->ExecuteSql(tail_query_partition_table));
            ZETASQL_RET_CHECK(!IsQueryResultEmpty(tail_partition_records_results));
            ZETASQL_ASSIGN_OR_RETURN(
                auto responses,
                ConvertPartitionTableRowCursorToStruct(
                    tail_partition_records_results.rows.get(),
                    /*initial_start_time=*/std::nullopt, expect_metadata));
            expect_metadata = false;
            for (auto& response : responses) {
              stream->Send(response);
            }
            return absl::OkStatus();
          }
          return absl::OkStatus();
        });
    ZETASQL_RETURN_IF_ERROR(status);
    // Increment by 1 microsecond gap to avoid repetitive records.
    current_start = scan_end + absl::Microseconds(1);
    current_end = std::min(
        {current_start +
             absl::GetFlag(FLAGS_change_streams_partition_query_chop_interval),
         tvf_end, partition_token_end_time});
  }

  // If expect_metadata is still true, stub a heartbeat record.
  if (expect_metadata == true) {
    ZETASQL_ASSIGN_OR_RETURN(auto extra_heartbeat, ConvertHeartbeatTimestampToStruct(
                                               tvf_end, expect_metadata));
    for (auto& response : extra_heartbeat) {
      stream->Send(response);
    }
  }
  return absl::OkStatus();
}

absl::Status ChangeStreamsHandler::ExecuteChangeStreamQuery(
    const spanner_api::ExecuteSqlRequest* request,
    ServerStream<spanner_api::PartialResultSet>* stream,
    std::shared_ptr<Session> session) {
  ZETASQL_RETURN_IF_ERROR(
      ValidateTransactionSelectorForChangeStreamQuery(request->transaction()));
  if (request->query_mode() == spanner_api::ExecuteSqlRequest::PLAN) {
    return error::EmulatorDoesNotSupportQueryPlans();
  }
  if (!metadata().partition_token.has_value()) {
    return ExecuteInitialQuery(session, stream);
  } else {
    return ExecutePartitionQuery(stream, session);
  }
}
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
