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

#include "backend/datamodel/types.h"

#include "zetasql/public/type.pb.h"
#include "common/feature_flags.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

bool IsSupportedColumnType(const zetasql::Type* type) {
  // According to https://cloud.google.com/spanner/docs/data-types
  // Note: ZetaSQL currently doesn't support constructing array-of-array
  // types.
  if (type->IsArray()) {
    const zetasql::Type* element_type = type->AsArray()->element_type();
    if (element_type->IsArray()) {
      return false;
    }
    return IsSupportedColumnType(element_type);
  }
  switch (type->kind()) {
    case zetasql::TypeKind::TYPE_INT64:
    case zetasql::TypeKind::TYPE_BOOL:
    case zetasql::TypeKind::TYPE_DOUBLE:
    case zetasql::TypeKind::TYPE_STRING:
    case zetasql::TypeKind::TYPE_BYTES:
    case zetasql::TypeKind::TYPE_TIMESTAMP:
    case zetasql::TypeKind::TYPE_DATE:
    case zetasql::TypeKind::TYPE_NUMERIC:
    case zetasql::TypeKind::TYPE_JSON:
      return true;
    default:
      return false;
  }
}

bool IsSupportedKeyColumnType(const zetasql::Type* type) {
  // According to https://cloud.google.com/spanner/docs/data-types
  if (type->IsArray() || type->IsJson()) {
    return false;
  }
  return IsSupportedColumnType(type);
}

std::string ToString(const zetasql::Type* type) {
  return type->ShortTypeName(zetasql::PRODUCT_EXTERNAL);
}

const zetasql::Type* BaseType(const zetasql::Type* type) {
  if (!IsSupportedColumnType(type)) {
    return nullptr;
  }
  return type->IsArray() ? type->AsArray()->element_type() : type;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
