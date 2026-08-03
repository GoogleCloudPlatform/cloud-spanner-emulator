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
#include "absl/status/status.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/updater/global_schema_names.h"
#include "common/errors.h"
#include "common/limits.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {}  // namespace

absl::Status ChangeStreamValidator::Validate(const ChangeStream* change_stream,
                                             SchemaValidationContext* context) {
  GOOGLESQL_RET_CHECK(!change_stream->name_.empty());
  if (context->is_postgresql_dialect()) {
    GOOGLESQL_RET_CHECK(change_stream->postgresql_oid().has_value());
  } else {
    GOOGLESQL_RET_CHECK(!change_stream->postgresql_oid().has_value());
  }
  GOOGLESQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName("Change Stream",
                                                        change_stream->name_));
  // TODO: Validate TVF name.
  return absl::OkStatus();
}

// TODO: Implement ValidateUpdate and add unit tests.
absl::Status ChangeStreamValidator::ValidateUpdate(
    const ChangeStream* change_stream, const ChangeStream* old_change_stream,
    SchemaValidationContext* context) {
  if (change_stream->is_deleted()) {
    GOOGLESQL_RET_CHECK(change_stream->change_stream_data_table_->is_deleted());
    GOOGLESQL_RET_CHECK(change_stream->change_stream_partition_table_->is_deleted());
    context->global_names()->RemoveName(change_stream->Name());
    return absl::OkStatus();
  }
  GOOGLESQL_RET_CHECK(!change_stream->change_stream_data_table()->is_deleted());
  GOOGLESQL_RET_CHECK(!change_stream->change_stream_partition_table()->is_deleted());
  GOOGLESQL_RET_CHECK_EQ(change_stream->Name(), old_change_stream->Name());
  GOOGLESQL_RET_CHECK_EQ(change_stream->id(), old_change_stream->id());
  if (context->is_postgresql_dialect()) {
    GOOGLESQL_RET_CHECK(change_stream->postgresql_oid().has_value());
    GOOGLESQL_RET_CHECK(old_change_stream->postgresql_oid().has_value());
    GOOGLESQL_RET_CHECK_EQ(change_stream->postgresql_oid().value(),
                 old_change_stream->postgresql_oid().value());
  } else {
    GOOGLESQL_RET_CHECK(!change_stream->postgresql_oid().has_value());
    GOOGLESQL_RET_CHECK(!old_change_stream->postgresql_oid().has_value());
  }

  std::string old_partition_mode = old_change_stream->partition_mode().value_or(
      std::string(kChangeStreamPartitionModeImmutableKeyRange));
  std::string new_partition_mode = change_stream->partition_mode().value_or(
      std::string(kChangeStreamPartitionModeImmutableKeyRange));
  if (old_partition_mode != new_partition_mode) {
    return error::AlterChangeStreamPartitionModeNotAllowed(
        change_stream->Name(), old_partition_mode);
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
