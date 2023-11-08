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

#include <string>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "common/feature_flags.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using ::google::spanner::v1::TypeAnnotationCode;
using ::postgres_translator::spangres::datatypes::SpannerExtendedType;

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
    case zetasql::TypeKind::TYPE_EXTENDED: {
      auto type_code = static_cast<const SpannerExtendedType*>(type)->code();
      switch (type_code) {
        case TypeAnnotationCode::PG_JSONB:
        case TypeAnnotationCode::PG_NUMERIC:
          return true;
        default:
          return false;
      }
    }
    default:
      return false;
  }
}

bool IsSupportedKeyColumnType(const zetasql::Type* type) {
  // According to https://cloud.google.com/spanner/docs/data-types
  if (type->IsArray() || type->IsJson()) {
    return false;
  }
  // PG.NUMERIC and JSONB do not support Primary/Foreign Key according to
  // https://cloud.google.com/spanner/docs/working-with-jsonb#unsupported_jsonb_features
  // and
  // https://cloud.google.com/spanner/docs/working-with-numerics#postgresql-numeric
  if (type->IsExtendedType()) {
    auto type_code = static_cast<const SpannerExtendedType*>(type)->code();
    if (type_code == TypeAnnotationCode::PG_NUMERIC ||
        type_code == TypeAnnotationCode::PG_JSONB) {
      return false;
    }
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
