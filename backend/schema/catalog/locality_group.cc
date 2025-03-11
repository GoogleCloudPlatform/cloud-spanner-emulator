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

#include "backend/schema/catalog/locality_group.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status LocalityGroup::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status LocalityGroup::ValidateUpdate(
    const SchemaNode* old, SchemaValidationContext* context) const {
  return validate_update_(this, old->template As<const LocalityGroup>(),
                          context);
}

std::string LocalityGroup::DebugString() const {
  return absl::Substitute(
      "LocalityGroup:$0 (inflash: $1, ssd_to_hdd_spill_timespans: $2)", name_,
      inflash_ ? "true" : "false",
      absl::StrJoin(ssd_to_hdd_spill_timespans_, ","));
}

absl::Status LocalityGroup::DeepClone(SchemaGraphEditor* editor,
                                      const SchemaNode* orig) {
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
