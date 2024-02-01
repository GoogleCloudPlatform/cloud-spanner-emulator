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

#include "backend/schema/catalog/database_options.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
absl::Status DatabaseOptions::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status DatabaseOptions::ValidateUpdate(
    const SchemaNode* orig, SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const DatabaseOptions>(), context);
}

std::string DatabaseOptions::DebugString() const {
  return absl::Substitute("I:$0[$1]", Name(), database_name_);
}

absl::Status DatabaseOptions::DeepClone(SchemaGraphEditor* editor,
                                        const SchemaNode* orig) {
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
