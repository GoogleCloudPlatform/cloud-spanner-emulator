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

#include "backend/schema/catalog/view.h"

#include <string>

#include "zetasql/public/type.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status View::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status View::ValidateUpdate(const SchemaNode* orig,
                                  SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const View>(), context);
}

absl::Status View::DeepClone(SchemaGraphEditor* editor,
                             const SchemaNode* orig) {
  // We just need to propagate the clone event to dependencies.
  return editor->CloneVector(&dependencies_);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
