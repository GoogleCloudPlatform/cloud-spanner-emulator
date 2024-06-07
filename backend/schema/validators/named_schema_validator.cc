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

#include "backend/schema/validators/named_schema_validator.h"

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

// Get all the names of objects in a vector of objects. Useful for creating
// error messages with multiple objects, i.e. getting all the names of tables in
// a schema.
template <typename T>
std::vector<std::string> GetObjectNames(absl::Span<const T* const> objects) {
  std::vector<std::string> names;
  for (const auto* object : objects) {
    names.push_back(object->Name());
  }
  return names;
}
}  // namespace

absl::Status NamedSchemaValidator::Validate(const NamedSchema* named_schema,
                                            SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!named_schema->name_.empty());
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(named_schema->postgresql_oid().has_value());
  } else {
    ZETASQL_RET_CHECK(!named_schema->postgresql_oid().has_value());
  }
  ZETASQL_RETURN_IF_ERROR(
      GlobalSchemaNames::ValidateSchemaName("Schema", named_schema->name_));
  return absl::OkStatus();
}

absl::Status NamedSchemaValidator::ValidateUpdate(
    const NamedSchema* named_schema, const NamedSchema* old_named_schema,
    SchemaValidationContext* context) {
  if (named_schema->is_deleted()) {
    if (!named_schema->tables().empty() || !named_schema->views().empty() ||
        !named_schema->indexes().empty() ||
        !named_schema->sequences().empty()) {
      return error::DropNamedSchemaHasDependencies(
          named_schema->Name(), GetObjectNames(named_schema->tables()),
          GetObjectNames(named_schema->views()),
          GetObjectNames(named_schema->indexes()),
          GetObjectNames(named_schema->sequences()));
    }
    context->global_names()->RemoveName(named_schema->Name());
    return absl::OkStatus();
  }

  ZETASQL_RET_CHECK_EQ(named_schema->Name(), old_named_schema->Name());
  ZETASQL_RET_CHECK_EQ(named_schema->id(), old_named_schema->id());
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(named_schema->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(old_named_schema->postgresql_oid().has_value());
    ZETASQL_RET_CHECK_EQ(named_schema->postgresql_oid().value(),
                 old_named_schema->postgresql_oid().value());
  } else {
    ZETASQL_RET_CHECK(!named_schema->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(!old_named_schema->postgresql_oid().has_value());
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
