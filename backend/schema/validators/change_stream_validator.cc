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

#include "backend/schema/validators/change_stream_validator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/global_schema_names.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {}  // namespace

absl::Status ChangeStreamValidator::Validate(const ChangeStream* change_stream,
                                             SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!change_stream->name_.empty());
  ZETASQL_RET_CHECK(!change_stream->id_.empty());
  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName("Change Stream",
                                                        change_stream->name_));

  return absl::OkStatus();
}

// TODO: Implement ValidateUpdate and add unit tests.
absl::Status ChangeStreamValidator::ValidateUpdate(
    const ChangeStream* change_stream, const ChangeStream* old_change_stream,
    SchemaValidationContext* context) {
  return absl::OkStatus();
}

// TODO: Return error when too many change streams tracking the
// same column.
absl::Status ChangeStreamValidator::ValidateLimits() {
  if (create_.for_clause().all()) {
    int all_count = 1;
    for (const std::shared_ptr<const ChangeStream>& change_stream :
         schema_.change_streams()) {
      if (change_stream->Name() != create_.change_stream_name() &&
          change_stream->for_clause()->all()) {
        ++all_count;
        // Number of change streams tracking ALL should not exceed the limit.
        if (all_count > limits::kMaxChangeStreamsTrackingATableOrColumn) {
          return error::TooManyChangeStreamsTrackingSameObject(
              create_.change_stream_name(),
              limits::kMaxChangeStreamsTrackingATableOrColumn, "ALL");
        }
      }
    }
  }

  // Checks the number of change streams tracking the same table.
  auto validate_table = [this](Table* table) {
    absl::flat_hash_set<std::string> all_change_streams;
    all_change_streams.reserve(table->change_streams().size());
    for (const auto& change_stream : table->change_streams()) {
      all_change_streams.insert(change_stream->Name());
    }

    int change_stream_count = table->change_streams().size();
    if (!all_change_streams.contains(create_.change_stream_name())) {
      ++change_stream_count;
    }
    if (change_stream_count > limits::kMaxChangeStreamsTrackingATableOrColumn) {
      return error::TooManyChangeStreamsTrackingSameObject(
          create_.change_stream_name(),
          limits::kMaxChangeStreamsTrackingATableOrColumn, table->Name());
    }
    return absl::OkStatus();
  };

  ZETASQL_RETURN_IF_ERROR(ForEachTrackedObject(create_, schema_, validate_table));
  return absl::OkStatus();
}

// TODO: Implement ForEachTrackedObject.
absl::Status ChangeStreamValidator::ForEachTrackedObject(
    const ddl::CreateChangeStream& create, Schema& schema,
    absl::FunctionRef<absl::Status(Table*)> table_cb) {
  return absl::OkStatus();
}

absl::Status ChangeStreamValidator::ValidateForClause() {
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
