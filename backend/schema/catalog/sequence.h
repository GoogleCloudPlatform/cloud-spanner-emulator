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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SEQUENCE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SEQUENCE_H_
#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/constants.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Sequence : public SchemaNode {
 public:
  // Returns the name of this sequence.
  std::string Name() const { return name_; }
  const std::shared_ptr<Schema> schema() const { return schema_; }
  std::optional<int64_t> start_with_counter() const { return start_with_; }
  std::optional<int64_t> skip_range_min() const { return skip_range_min_; }
  std::optional<int64_t> skip_range_max() const { return skip_range_max_; }

  ~Sequence() { RemoveSequenceFromLastValuesMap(); }

  // Returns a unique id of this sequence.
  const SequenceID id() const { return id_; }

  enum SequenceKind { BIT_REVERSED_POSITIVE = 0 };

  SequenceKind sequence_kind() const { return sequence_kind_; }

  std::string sequence_kind_name() const {
    if (sequence_kind_ == SequenceKind::BIT_REVERSED_POSITIVE) {
      return "BIT_REVERSED_POSITIVE";
    }
    return "INVALID";
  }

  inline static absl::flat_hash_map<std::string, int64_t> SequenceLastValues;

  // Returns the next sequence value according to the sequence kind.
  absl::StatusOr<zetasql::Value> GetNextSequenceValue() const;

  // Returns the internal current counter of the sequence.
  zetasql::Value GetInternalSequenceState() const;

  // Reset the sequence's last value to the schema's current start_with_.
  void ResetSequenceLastValue() const;

  // SchemaNode interface implementation.
  // ------------------------------------
  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Sequence", .global = true};
  }
  absl::Status Validate(SchemaValidationContext* context) const override;
  absl::Status ValidateUpdate(const SchemaNode* old,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;
  class Builder;
  class Editor;

 private:
  friend class SequenceValidator;

  using ValidationFn =
      std::function<absl::Status(const Sequence*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const Sequence*, const Sequence*, SchemaValidationContext*)>;

  // Constructors are private and only friend classes are able to build.
  Sequence(const ValidationFn& validate,
           const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}

  Sequence(const Sequence&) = default;

  void RemoveSequenceFromLastValuesMap() const;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Sequence(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;
  // Validation delegates.
  const ValidationFn validate_;
  const UpdateValidationFn validate_update_;

  // Name of this sequence.
  std::string name_;

  // A unique ID for identifying this sequence in the schema that owns this
  // sequence.
  SequenceID id_;

  std::shared_ptr<Schema> schema_ = nullptr;

  SequenceKind sequence_kind_;
  std::optional<int64_t> start_with_;
  std::optional<int64_t> skip_range_min_;
  std::optional<int64_t> skip_range_max_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SEQUENCE_H_
