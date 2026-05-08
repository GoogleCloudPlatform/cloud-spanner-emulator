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
#include "googlesql/public/options.pb.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/type.h"
#include "common/feature_flags.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using ::google::spanner::v1::TypeAnnotationCode;
using ::postgres_translator::spangres::datatypes::SpannerExtendedType;

bool IsSupportedColumnType(const googlesql::Type* type) {
  // According to https://cloud.google.com/spanner/docs/data-types
  // Note: GoogleSQL currently doesn't support constructing array-of-array
  // types.
  if (type->IsArray()) {
    const googlesql::Type* element_type = type->AsArray()->element_type();
    if (element_type->IsArray()) {
      return false;
    }
    return IsSupportedColumnType(element_type);
  }
  switch (type->kind()) {
    case googlesql::TypeKind::TYPE_INT64:
    case googlesql::TypeKind::TYPE_BOOL:
    case googlesql::TypeKind::TYPE_FLOAT:
    case googlesql::TypeKind::TYPE_DOUBLE:
    case googlesql::TypeKind::TYPE_STRING:
    case googlesql::TypeKind::TYPE_BYTES:
    case googlesql::TypeKind::TYPE_TIMESTAMP:
    case googlesql::TypeKind::TYPE_DATE:
    case googlesql::TypeKind::TYPE_NUMERIC:
    case googlesql::TypeKind::TYPE_JSON:
    case googlesql::TypeKind::TYPE_TOKENLIST:
    case googlesql::TypeKind::TYPE_UUID:
      return true;
    case googlesql::TypeKind::TYPE_PROTO:
    case googlesql::TypeKind::TYPE_ENUM: {
      return EmulatorFeatureFlags::instance().flags().enable_protos;
    }
    case googlesql::TypeKind::TYPE_EXTENDED: {
      auto type_code = static_cast<const SpannerExtendedType*>(type)->code();
      switch (type_code) {
        case TypeAnnotationCode::PG_JSONB:
        case TypeAnnotationCode::PG_NUMERIC:
          return true;
        default:
          return false;
      }
    }
    // INTERVAL is a query only type.
    case googlesql::TypeKind::TYPE_INTERVAL:
      return false;
    default:
      return false;
  }
}

bool IsSupportedKeyColumnType(const googlesql::Type* type,
                              bool is_vector_index) {
  // According to
  // https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-types#valid_key_column_types
  if (type->IsJson() || type->IsFloat()) {
    return false;
  }
  if (type->IsArray() && !is_vector_index) {
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
  if (type->IsTokenList()) {
    return false;
  }
  return IsSupportedColumnType(type);
}

std::string ToString(const googlesql::Type* type) {
  return type->ShortTypeName(googlesql::PRODUCT_EXTERNAL,
                             /*use_external_float32=*/true);
}

const googlesql::Type* BaseType(const googlesql::Type* type) {
  if (!IsSupportedColumnType(type)) {
    return nullptr;
  }
  return type->IsArray() ? type->AsArray()->element_type() : type;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
