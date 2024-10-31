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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_NAMED_SCHEMA_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_NAMED_SCHEMA_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class NamedSchema : public SchemaNode {
 public:
  // Returns the name of the named schema.
  std::string Name() const { return name_; }

  // Returns the unique ID of this named schema.
  const NamedSchemaID id() const { return id_; }

  absl::Span<const Table* const> tables() const { return tables_; }
  absl::Span<const View* const> views() const { return views_; }
  absl::Span<const Index* const> indexes() const { return indexes_; }
  absl::Span<const Sequence* const> sequences() const { return sequences_; }
  absl::Span<const Udf* const> udfs() const { return udfs_; }

  // Finds a Table by name. Returns nullptr if the named schema doesn't contain
  // a Table called "table_name."
  const Table* FindTable(const std::string& table_name) const;

  const Table* FindTableUsingSynonym(const std::string& table_synonym) const;

  // Finds a View by name. Returns nullptr if the named schema doesn't contain a
  // View called "view_name."
  const View* FindView(const std::string& view_name) const;

  // Finds a Index by name. Returns nullptr if the named schema doesn't contain
  // a Index called "index_name."
  const Index* FindIndex(const std::string& index_name) const;

  // Finds a Sequence by name. Returns nullptr if the named schema doesn't
  // contain a Sequence called "sequence_name."
  const Sequence* FindSequence(const std::string& sequence_name) const;

  // Finds a Udf by name. Returns nullptr if the named schema doesn't contain a
  // Udf called "udf_name."
  const Udf* FindUdf(const std::string& udf_name) const;

  // SchemaNode interface implementation.
  // ------------------------------------
  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "NamedSchema", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("S:$0[$1]", Name(), id_);
  }

  class Builder;
  class Editor;

 private:
  friend class NamedSchemaValidator;

  using ValidationFn =
      std::function<absl::Status(const NamedSchema*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const NamedSchema*, const NamedSchema*, SchemaValidationContext*)>;

  // Constructors are private and only friend classes are able to build /
  // modify.
  NamedSchema(const ValidationFn& validate,
              const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  NamedSchema(const NamedSchema&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new NamedSchema(*this));
  }

  template <typename T>
  absl::Status CloneOrDeleteSchemaObjects(
      SchemaGraphEditor* editor, std::vector<const T*>& schema_objects,
      CaseInsensitiveStringMap<const T*>& schema_objects_map);

  absl::Status CloneOrDeleteSchemaObjects(
      SchemaGraphEditor* editor, std::vector<const Table*>& schema_objects,
      CaseInsensitiveStringMap<const Table*>& schema_objects_map);

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;
  const UpdateValidationFn validate_update_;

  // The name of this named schema.
  std::string name_;

  // A unique ID for identifying this named schema in the database that owns it.
  NamedSchemaID id_;

  std::vector<const Table*> tables_;
  CaseInsensitiveStringMap<const Table*> tables_map_;

  std::vector<const Table*> synonyms_;
  CaseInsensitiveStringMap<const Table*> synonyms_map_;

  std::vector<const View*> views_;
  CaseInsensitiveStringMap<const View*> views_map_;

  std::vector<const Index*> indexes_;
  CaseInsensitiveStringMap<const Index*> indexes_map_;

  std::vector<const Sequence*> sequences_;
  CaseInsensitiveStringMap<const Sequence*> sequences_map_;

  std::vector<const Udf*> udfs_;
  CaseInsensitiveStringMap<const Udf*> udfs_map_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_NAMED_SCHEMA_H_
