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

#include "backend/schema/validators/database_options_validator.h"

#include "absl/status/status.h"
#include "backend/schema/catalog/database_options.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {}  // namespace

absl::Status DatabaseOptionsValidator::Validate(
    const DatabaseOptions* database_options, SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!database_options->database_name_.empty());
  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName(
      "Database", database_options->database_name_));
  return absl::OkStatus();
}

// TODO: Implement ValidateUpdate and add unit tests.
absl::Status DatabaseOptionsValidator::ValidateUpdate(
    const DatabaseOptions* database_options,
    const DatabaseOptions* old_database_options,
    SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_EQ(database_options->Name(), old_database_options->Name());
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
