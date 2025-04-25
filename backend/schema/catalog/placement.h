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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PLACEMENT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PLACEMENT_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Table;
class Column;
class Placement : public SchemaNode {
 public:
  // Returns the name of this placement.
  std::string PlacementName() const { return name_; }
  // Returns the options of this placement.
  ::google::protobuf::RepeatedPtrField<ddl::SetOption> options() const {
    return options_;
  }
  // Returns the default leader of this placement.
  std::optional<std::string> DefaultLeader() const { return default_leader_; }
  // Returns the instance partition of this placement.
  std::optional<std::string> InstancePartition() const {
    return instance_partition_;
  }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Placement", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* old,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;

  bool HasExplicitValidOptions() const {
    return default_leader_.has_value() || instance_partition_.has_value();
  }
  class Builder;
  class Editor;

 private:
  friend class PlacementValidator;
  using ValidationFn =
      std::function<absl::Status(const Placement*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const Placement*, const Placement*, SchemaValidationContext*)>;
  // Constructors are private and only friend classes are able to build.
  Placement(const ValidationFn& validate,
            const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  Placement(const Placement&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Placement(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // Name of this placement.
  std::string name_;

  // Options for this placement.
  ::google::protobuf::RepeatedPtrField<ddl::SetOption> options_;

  // default leader for this placement.
  std::optional<std::string> default_leader_;

  // instance partition for this placement.
  std::optional<std::string> instance_partition_;
};
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PLACEMENT_H_
