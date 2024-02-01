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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_DATABASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_DATABASE_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
class Table;

class DatabaseOptions : public SchemaNode {
 public:
  // Returns the name of this database.
  std::string Name() const { return database_name_; }

  const ddl::SetOption* options() const { return options_; }

  // SchemaNode interface implementation.
  // ------------------------------------
  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{
        .name = database_name_, .kind = "DatabaseOptions", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;
  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;
  class Builder;
  class Editor;

 private:
  friend class DatabaseOptionsValidator;
  using ValidationFn = std::function<absl::Status(const DatabaseOptions*,
                                                  SchemaValidationContext*)>;
  using UpdateValidationFn =
      std::function<absl::Status(const DatabaseOptions*, const DatabaseOptions*,
                                 SchemaValidationContext*)>;
  // Constructors are private and only friend classes are able to build.
  DatabaseOptions(const ValidationFn& validate,
                  const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  DatabaseOptions(const DatabaseOptions&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new DatabaseOptions(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // Name of this database.
  std::string database_name_;
  // TODO: Add functionality to update schema.
  const ddl::SetOption* options_ = nullptr;
};
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_DATABASE_H_
