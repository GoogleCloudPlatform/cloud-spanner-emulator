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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_LOCALITY_GROUP_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_LOCALITY_GROUP_H_

#include <stdbool.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Column;
class Table;

// LocalityGroup represents a locality group in a database.
class LocalityGroup : public SchemaNode {
 public:
  // Returns the name of this locality group.
  std::string Name() const { return name_; }

  // Returns the number of tables and columns that are in this locality group.
  std::int64_t use_count() const { return use_count_; }

  std::optional<bool> inflash() const { return inflash_; }
  ::google::protobuf::RepeatedPtrField<std::string> ssd_to_hdd_spill_timespans() const {
    return ssd_to_hdd_spill_timespans_;
  }

  ::google::protobuf::RepeatedPtrField<ddl::SetOption> options() const {
    return options_;
  }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{
        .name = name_, .kind = "LocalityGroup", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* old,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;
  class Builder;
  class Editor;

 private:
  friend class LocalityGroupValidator;
  using ValidationFn = std::function<absl::Status(const LocalityGroup*,
                                                  SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const LocalityGroup*, const LocalityGroup*, SchemaValidationContext*)>;
  // Constructors are private and only friend classes are able to build.
  LocalityGroup(const ValidationFn& validate,
                const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  LocalityGroup(const LocalityGroup&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new LocalityGroup(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;
  const UpdateValidationFn validate_update_;

  // The name of this locality group.
  std::string name_;

  // The number of tables and columns that are in this locality group.
  std::int64_t use_count_ = 0;

  // The storage type is inflash or not.
  std::optional<bool> inflash_;

  // The ssd to hdd spill timespan of this locality group.
  ::google::protobuf::RepeatedPtrField<std::string> ssd_to_hdd_spill_timespans_;

  // The set of options for this locality group.
  ::google::protobuf::RepeatedPtrField<ddl::SetOption> options_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_LOCALITY_GROUP_H_
