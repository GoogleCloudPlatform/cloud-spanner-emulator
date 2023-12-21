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

#include "backend/schema/validators/sequence_validator.h"

#include <string>

#include "absl/status/status.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status SequenceValidator::Validate(const Sequence* sequence,
                                         SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!sequence->name_.empty());
  ZETASQL_RETURN_IF_ERROR(
      GlobalSchemaNames::ValidateSchemaName("Sequence", sequence->name_));
  return absl::OkStatus();
}

absl::Status SequenceValidator::ValidateUpdate(
    const Sequence* sequence, const Sequence* old_sequence,
    SchemaValidationContext* context) {
  if (sequence->is_deleted()) {
    context->global_names()->RemoveName(sequence->Name());
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK_EQ(sequence->Name(), old_sequence->Name());
  ZETASQL_RET_CHECK_EQ(sequence->sequence_kind(), old_sequence->sequence_kind());
  ZETASQL_RET_CHECK_EQ(sequence->id(), old_sequence->id());

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
