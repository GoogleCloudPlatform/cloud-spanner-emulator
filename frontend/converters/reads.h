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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_READS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_READS_H_

#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/options.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

zetasql_base::StatusOr<backend::ReadOnlyOptions> ReadOnlyOptionsFromProto(
    const google::spanner::v1::TransactionOptions::ReadOnly& proto);

// Populates a ReadArg from a ReadRequest proto.
absl::Status ReadArgFromProto(const backend::Schema& schema,
                              const google::spanner::v1::ReadRequest& request,
                              backend::ReadArg* read_arg);

// Converts a RowCursor to a ResultSet proto.
//
// Only handles the types and values supported by Cloud Spanner. Invalid types
// or values not supported by Cloud Spanner will return errors. If limit > 0,
// will only convert first limit numbers of rows into result_pb.
absl::Status RowCursorToResultSetProto(
    backend::RowCursor* cursor, int limit,
    google::spanner::v1::ResultSet* result_pb);

// Converts a RowCursor to a set of one or more PartialResultSet protos.
//
// Only handles the types and values supported by Cloud Spanner. Invalid types
// or values not supported by Cloud Spanner will return errors. If limit > 0,
// will only convert first limit numbers of rows. If the results exceed the max
// streaming chunk size for a given partial result set, it will be chunked into
// multiple partial result sets.
zetasql_base::StatusOr<std::vector<google::spanner::v1::PartialResultSet>>
RowCursorToPartialResultSetProtos(backend::RowCursor* cursor, int limit);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_READS_H_
