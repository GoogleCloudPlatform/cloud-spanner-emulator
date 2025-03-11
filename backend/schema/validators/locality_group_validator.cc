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

#include "backend/schema/validators/locality_group_validator.h"

#include "absl/status/status.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status LocalityGroupValidator::Validate(
    const LocalityGroup* locality_group, SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!locality_group->Name().empty());
  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName(
      "Locality Group", locality_group->Name()));

  return absl::OkStatus();
}

absl::Status LocalityGroupValidator::ValidateUpdate(
    const LocalityGroup* locality_group,
    const LocalityGroup* old_locality_group, SchemaValidationContext* context) {
  if (locality_group->is_deleted()) {
    ZETASQL_RET_CHECK_EQ(locality_group->use_count(), 0);
    context->global_names()->RemoveName(locality_group->Name());
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK_EQ(locality_group->Name(), old_locality_group->Name());
  return absl::OkStatus();
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
