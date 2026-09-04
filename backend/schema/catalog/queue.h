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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_QUEUE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_QUEUE_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Queue : public SchemaNode {
 public:
  // Returns the name of the queue.
  std::string Name() const { return name_; }

  // Returns the unique ID of this queue.
  const QueueID id() const { return id_; }

  // Returns the list of all columns of this queue.
  absl::Span<const Column* const> columns() const { return columns_; }

  // Returns the primary key of this queue.
  const absl::Span<const KeyColumn* const> primary_key() const {
    return primary_key_;
  }

  // Returns the parent table of this queue, or nullptr if this queue does not
  // have a parent table.
  const Table* parent() const { return parent_table_; }

  // Returns the on delete action of this queue.
  Table::OnDeleteAction on_delete_action() const {
    return on_delete_action_.value_or(Table::OnDeleteAction::kNoAction);
  }

  // Returns true if the on delete action of this queue is set.
  bool has_on_delete_action() const { return on_delete_action_.has_value(); }

  // Returns the row deletion policy of this queue.
  std::optional<ddl::RowDeletionPolicy> row_deletion_policy() const {
    return row_deletion_policy_;
  }

  // Returns the options set on this queue.
  absl::Span<const ddl::SetOption> options() const { return options_; }

  // Finds a column by its name. Returns a const pointer to the column, or
  // nullptr if the column is not found. Name comparison is case-insensitive.
  const Column* FindColumn(const std::string& column_name) const;

  // Same as above, but name comparison is case-sensitive.
  const Column* FindColumnCaseSensitive(const std::string& column_name) const;

  // Finds a KeyColumn by name. Returns nullptr if queue doesn't contain
  // a column named `column_name` or if it's not a key column.
  const KeyColumn* FindKeyColumn(const std::string& column_name) const;

  // SchemaNode interface implementation.
  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Queue", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("Q:$0[$1]", Name(), id_);
  }

  class Builder;
  class Editor;

 private:
  friend class QueueValidator;

  // Constructors are private.
  Queue(const std::function<absl::Status(const Queue*,
                                         SchemaValidationContext*)>& validate,
        const std::function<absl::Status(const Queue*, const Queue*,
                                         SchemaValidationContext*)>&
            validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  Queue(const Queue&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Queue(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const std::function<absl::Status(const Queue*, SchemaValidationContext*)>
      validate_;
  const std::function<absl::Status(const Queue*, const Queue*,
                                   SchemaValidationContext*)>
      validate_update_;

  std::string name_;
  QueueID id_;
  std::vector<const Column*> columns_;
  CaseInsensitiveStringMap<const Column*> columns_map_;
  std::vector<const KeyColumn*> primary_key_;
  const Table* parent_table_ = nullptr;
  std::optional<Table::OnDeleteAction> on_delete_action_ = std::nullopt;
  std::optional<ddl::RowDeletionPolicy> row_deletion_policy_ = std::nullopt;
  std::vector<ddl::SetOption> options_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_QUEUE_H_
