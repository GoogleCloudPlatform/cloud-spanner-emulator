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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_CONTEXT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_CONTEXT_H_

#include <optional>

#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/commit_timestamp.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// QueryContext provides resources required to execute a query.
struct QueryContext {
  // The database schema.
  const Schema* schema = nullptr;

  // A reader for reading data.
  RowReader* reader = nullptr;

  // A writer for writing data for DML requests. Can be null for SELECT queries.
  RowWriter* writer = nullptr;

  // Tracks tables/columns containing pending commit timestamps.
  const CommitTimestampTracker* commit_timestamp_tracker = nullptr;

  // True if the context of this query allows functions that need read-write
  // transactions. E.g. when analyzing a column expression, in partitioned DML,
  // or read-write transactions.
  bool allow_read_write_only_functions = false;

  // nullopt if it hasn't been set, true if the query is executed in the
  // context of a read-only transaction and false if the query is executed in
  // the context of a read-write transaction.
  std::optional<bool> is_read_only_txn = std::nullopt;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_CONTEXT_H_
