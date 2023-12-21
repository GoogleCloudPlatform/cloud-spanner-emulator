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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_MODEL_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_MODEL_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/types/type.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Model : public SchemaNode {
 public:
  struct ModelColumn {
    std::string name;
    const zetasql::Type* type = nullptr;
    bool is_explicit;
    std::optional<bool> is_required;
  };

  const std::string& Name() const { return name_; }
  bool is_remote() const { return is_remote_; }

  absl::Span<const ModelColumn> input() const { return input_; }
  absl::Span<const ModelColumn> output() const { return output_; }

  const std::optional<std::string>& endpoint() const { return endpoint_; }
  const std::vector<std::string>& endpoints() const { return endpoints_; }
  const std::optional<int64_t> default_batch_size() const {
    return default_batch_size_;
  }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Model", .global = true};
  }
  absl::Status Validate(SchemaValidationContext* context) const override;
  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;
  class Builder;
  class Editor;

 private:
  friend class ModelValidator;
  using ValidationFn =
      std::function<absl::Status(const Model*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const Model*, const Model*, SchemaValidationContext*)>;
  // Constructors are private and only friend classes are able to build.
  Model(const ValidationFn& validate, const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  Model(const Model&) = default;
  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Model(*this));
  }
  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;
  const UpdateValidationFn validate_update_;

  std::string name_;
  bool is_remote_;
  std::vector<ModelColumn> input_;
  std::vector<ModelColumn> output_;
  // Options.
  std::optional<std::string> endpoint_;
  std::vector<std::string> endpoints_;
  std::optional<int64_t> default_batch_size_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_MODEL_H_
