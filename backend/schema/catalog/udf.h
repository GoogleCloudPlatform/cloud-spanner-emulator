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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_UDF_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_UDF_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Udf represents a UDF in a database.
class Udf : public SchemaNode {
 public:
  enum SqlSecurity {
    SQL_SECURITY_UNSPECIFIED,
    INVOKER,
  };

  enum Determinism {
    DETERMINISM_UNSPECIFIED,
    DETERMINISTIC,
    NOT_DETERMINISTIC_STABLE,
    NOT_DETERMINISTIC_VOLATILE,
  };

  // Returns the name of the UDF.
  const std::string& Name() const { return name_; }

  const std::string& body() const { return body_; }

  const zetasql::FunctionSignature* signature() const {
    return signature_.get();
  }

  Determinism determinism_level() const { return determinism_level_; }

  // Returns the sql body of the UDF in the original dialect.
  const std::optional<std::string>& body_origin() const { return body_origin_; }

  // A list of schema objects that UDF immediately depends on. A UDF can only
  // currently depend on tables, indexes, UDFs, columns, views, and sequences.
  absl::Span<const SchemaNode* const> dependencies() const {
    return dependencies_;
  }

  // A list of schema objects that immediately depend on this UDF. A UDF can
  // only currently be depended on by tables, indexes, udfs, views, columns, and
  // constraints.
  absl::Span<const SchemaNode* const> dependents() const { return dependents_; }

  SqlSecurity security() const { return security_; }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Udf", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("U:$0", Name());
  }

  class Builder;
  class Editor;

 private:
  friend class UdfValidator;

  using ValidationFn =
      std::function<absl::Status(const Udf*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const Udf*, const Udf*, SchemaValidationContext*)>;

  // Constructors are private and only friend classes are able to build /
  // modify.
  Udf(const ValidationFn& validate, const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  Udf(const Udf& other)
      : validate_(other.validate_),
        validate_update_(other.validate_update_),
        name_(other.name_),
        dependencies_(other.dependencies_),
        dependents_(other.dependents_),
        security_(other.security_),
        body_(other.body_),
        body_origin_(other.body_origin_),
        signature_(other.signature_
                       ? absl::make_unique<zetasql::FunctionSignature>(
                             *other.signature_)
                       : nullptr),
        determinism_level_(other.determinism_level_) {}

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Udf(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // The name of this UDF.
  std::string name_;

  // List of the SchemaNode(s) this UDF depends on.
  std::vector<const SchemaNode*> dependencies_;

  // List of the SchemaNode(s) that depend on this UDF.
  std::vector<const SchemaNode*> dependents_;

  // SQL Security mode of the UDF.
  SqlSecurity security_;

  // UDF definition body.
  std::string body_;

  // UDF definition body in the original dialect.
  std::optional<std::string> body_origin_;

  // The signature of the UDF.
  std::unique_ptr<zetasql::FunctionSignature> signature_ = nullptr;

  // The determinism level of the UDF.
  Determinism determinism_level_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_UDF_H_
