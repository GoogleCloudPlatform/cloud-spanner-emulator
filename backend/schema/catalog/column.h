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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_COLUMN_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_COLUMN_H_

#include <memory>
#include <string>

#include "zetasql/public/type.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/common/ids.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/limits.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Table;

// Column represents a column in a Table.
class Column : public SchemaNode {
 public:
  // Returns the name of the column.
  const std::string& Name() const { return name_; }

  // Qualified name of the column.
  std::string FullName() const;

  // Returns the type of the column.
  const zetasql::Type* GetType() const { return type_; }

  // Returns a unique id of this column.
  const ColumnID id() const { return id_; }

  // Returns true if this column allows commit timestamp to be atomically stored
  // on Commit.
  bool allows_commit_timestamp() const {
    return allows_commit_timestamp_.has_value() &&
           allows_commit_timestamp_.value();
  }

  // Returns true if the allow_commit_timestamp option was set explicitly on
  // this column.
  bool has_allows_commit_timestamp() const {
    return allows_commit_timestamp_.has_value();
  }

  // Returns true if this column allows null values.
  bool is_nullable() const { return is_nullable_; }

  // The length of a STRING or BYTES column, as declared in the schema. A
  // nullopt value represents the max allowed length for the column according to
  // https://cloud.google.com/spanner/docs/data-definition-language#scalars/
  absl::optional<int64_t> declared_max_length() const {
    return declared_max_length_;
  }

  // Returns the effective maximum length of values allowed in this
  // column, based on the type. Applicable only to STRING and BYTES types.
  int64_t effective_max_length() const {
    if (type_->IsString() ||
        (type_->IsArray() && type_->AsArray()->element_type()->IsString())) {
      return declared_max_length_.value_or(limits::kMaxStringColumnLength);
    }
    if (type_->IsBytes() ||
        (type_->IsArray() && type_->AsArray()->element_type()->IsBytes())) {
      return declared_max_length_.value_or(limits::kMaxBytesColumnLength);
    }
    return 0;
  }

  // Returns whether the column is a generated column.
  bool is_generated() const { return expression_.has_value(); }

  // Returns the expression if the column is a generated column.
  const absl::optional<std::string>& expression() const { return expression_; }

  absl::Span<const Column* const> dependent_columns() const {
    return dependent_columns_;
  }

  // Returns the source column.
  const Column* source_column() const { return source_column_; }

  // Returns the table containing the column.
  const Table* table() const { return table_; }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Column"};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("C:$0[$1]($2)", Name(), id_, type_->DebugString());
  }

  // Populates dependent_columns_.
  void PopulateDependentColumns();

  // TODO : Make external friend classes instead of nested classes.
  class Builder;
  class Editor;

 private:
  friend class ColumnValidator;

  using ValidationFn =
      std::function<absl::Status(const Column*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const Column*, const Column*, SchemaValidationContext*)>;

  // Constructors are private and only friend classes are able to build /
  // modify.
  Column(const ValidationFn& validate,
         const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  Column(const Column&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Column(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;
  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // Name of this column.
  std::string name_;

  // Unique ID of this column.
  ColumnID id_;

  // Type of this column.
  const zetasql::Type* type_;

  // The source column from the indexed table that this column is derived from.
  // Only used by index columns.
  const Column* source_column_ = nullptr;

  // Whether null values are allowed.
  bool is_nullable_ = true;

  // A tri state boolean indicating whether commit timestamp can be stored.
  // If allows_commit_timestamp is not set, it represents that the option isn't
  // set for the columns in the schema serialized back to the user.
  absl::optional<bool> allows_commit_timestamp_ = absl::nullopt;

  // Length for STRING and BYTES. If unset, indicates the max allowed length.
  absl::optional<int64_t> declared_max_length_ = absl::nullopt;

  // For a generated column, this is the generation expression.
  absl::optional<std::string> expression_ = absl::nullopt;

  // For a generated column, this is the list of columns that this column
  // references in its expression.
  std::vector<std::string> dependent_column_names_;
  std::vector<const Column*> dependent_columns_;

  // The table containing the column.
  const Table* table_ = nullptr;
};

// KeyColumn is a single column that is part of the key of a table or an index.
class KeyColumn : public SchemaNode {
 public:
  // Returns the column that this key column is based on.
  const Column* column() const { return column_; }

  // Returns true if this key column is sorted in descending order.
  bool is_descending() const { return is_descending_; }

  // SchemaNode interface implementation.
  // ------------------------------------

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("PK:$0(desc:$1)", column_->DebugString(),
                            is_descending_);
  }

  class Builder;

 private:
  friend class KeyColumnValidator;

  using ValidationFn =
      std::function<absl::Status(const KeyColumn*, SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const KeyColumn*, const KeyColumn*, SchemaValidationContext*)>;

  KeyColumn(const ValidationFn& validate,
            const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  KeyColumn(const KeyColumn&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new KeyColumn(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // The column that this KeyColumn is based on.
  const Column* column_;

  // Whether this key column is sorted in descending order.
  bool is_descending_ = false;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_COLUMN_H_
