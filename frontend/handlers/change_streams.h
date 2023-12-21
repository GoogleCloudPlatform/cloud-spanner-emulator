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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_HANDLERS_CHANGE_STREAMS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_HANDLERS_CHANGE_STREAMS_H_

#include <memory>
#include <string>

#include "google/spanner/v1/spanner.pb.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/query/change_stream/change_stream_query_validator.h"
#include "backend/query/query_engine.h"
#include "frontend/entities/session.h"
#include "frontend/server/handler.h"

ABSL_DECLARE_FLAG(bool, cloud_spanner_emulator_test_with_fake_partition_table);

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
// Sub-handler for change stream queries. There is no direct grpc request
// registered with this handler. Rather, if an incoming sql query is detected
// as a change stream query, we wire the query from the generic
// ExecuteStreamingSql handler to this specific handler.
class ChangeStreamsHandler {
 public:
  static constexpr char kTestPartitionTable[] = "partition_table";
  static constexpr char kTestDataTable[] = "data_table";
  explicit ChangeStreamsHandler(
      backend::ChangeStreamQueryValidator::ChangeStreamMetadata& metadata)
      : metadata_(metadata) {
    // Name of the partition&data table to be read from. For certain test cases
    // this need to be set to test only mock tables.
    partition_table_ =
        absl::GetFlag(
            FLAGS_cloud_spanner_emulator_test_with_fake_partition_table)
            ? kTestPartitionTable
            : metadata.partition_table;
  }

  absl::Status ExecuteChangeStreamQuery(
      const spanner_api::ExecuteSqlRequest* request,
      ServerStream<spanner_api::PartialResultSet>* stream,
      std::shared_ptr<Session> session);

  // Execute change stream initial query when partition token is null.
  absl::Status ExecuteInitialQuery(
      std::shared_ptr<Session> session,
      ServerStream<spanner_api::PartialResultSet>* stream);

  absl::StatusOr<absl::Time> TryGetPartitionTokenEndTime(
      std::shared_ptr<Session> session, absl::Time read_ts) const;

  // Execute change stream partition query when partition token is non null.
  absl::Status ExecutePartitionQuery(
      ServerStream<spanner_api::PartialResultSet>* stream,
      std::shared_ptr<Session> session);

  backend::Query ConstructPartitionTablePartitionQuery() const;

  backend::Query ConstructDataTablePartitionQuery(absl::Time start,
                                                  absl::Time end) const;

  absl::Status ProcessDataChangeRecordsAndStreamBack(
      backend::QueryResult& result, bool expect_heartbeat, absl::Time scan_end,
      bool& expect_metadata, absl::Time* last_record_time,
      ServerStream<spanner_api::PartialResultSet>* stream);

  const backend::ChangeStreamQueryValidator::ChangeStreamMetadata& metadata()
      const {
    return metadata_;
  }

 private:
  const backend::ChangeStreamQueryValidator::ChangeStreamMetadata& metadata_;
  std::string partition_table_;
};
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_HANDLERS_CHANGE_STREAMS_H_
