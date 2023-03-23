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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_VIEW_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_VIEW_H_

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class ForeignKey;
class Index;

// View represents a view in a database.
class View : public SchemaNode {
 public:
  struct Column {
    Column(const std::string& name, const zetasql::Type* type)
        : name(name), type(type) {}
    const std::string name;
    const zetasql::Type* const type = nullptr;
  };

  enum SqlSecurity {
    UNSPECIFIED,
    INVOKER,
  };

  // Returns the name of the view.
  const std::string& Name() const { return name_; }

  const std::string& body() const { return body_; }

  SqlSecurity security() const { return security_; }

  // Returns the list of all columns of this view.
  absl::Span<const View::Column> columns() const { return columns_; }

  // A list of schema objects that view immediately depends on. A view can only
  // currently depend on tables, columns, and other views.
  absl::Span<const SchemaNode* const> dependencies() const {
    return dependencies_;
  }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "View", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("V:$0", Name());
  }

  class Builder;
  class Editor;

 private:
  friend class ViewValidator;

  using ValidationFn =
      std::function<absl::Status(const View*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const View*, const View*, SchemaValidationContext*)>;

  // Constructors are private and only friend classes are able to build /
  // modify.
  View(const ValidationFn& validate, const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  View(const View&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new View(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // The name of this view.
  std::string name_;

  // List of the view's columns, in the same order as found in the view's DDL
  // definition.
  std::vector<View::Column> columns_;

  // A map of case-insensitive column names to the column values.
  CaseInsensitiveStringMap<View::Column> columns_map_;

  // List of the SchemaNode(s) corresponding to the dependencies in
  // `dependecy_names_`.
  std::vector<const SchemaNode*> dependencies_;

  // SQL SECURITY mode this view was created with.
  SqlSecurity security_;

  // View definition body.
  std::string body_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_VIEW_H_
