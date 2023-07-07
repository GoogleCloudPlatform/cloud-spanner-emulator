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

#include "backend/schema/catalog/change_stream.h"

#include <algorithm>
#include <string>
#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "common/limits.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status ChangeStream::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status ChangeStream::ValidateUpdate(
    const SchemaNode* old, SchemaValidationContext* context) const {
  return validate_update_(this, old->template As<const ChangeStream>(),
                          context);
}

std::string ChangeStream::DebugString() const {
  return absl::Substitute("I:$0[$1]", name_, change_stream_data_table_->Name());
}

absl::Status ChangeStream::DeepClone(SchemaGraphEditor* editor,
                                     const SchemaNode* orig) {
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
