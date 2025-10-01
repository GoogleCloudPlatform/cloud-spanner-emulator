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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "absl/log/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/graph/schema_node.h"
#include "common/limits.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Table;
class ChangeStream;

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
  std::optional<int64_t> declared_max_length() const {
    return declared_max_length_;
  }
  // Return true if vector length was set explicitly on the column.
  bool has_vector_length() const { return vector_length_.has_value(); }

  // Return the vector length of the array column.
  // A nullopt value means the vector length is not explicitly set.
  std::optional<uint32_t> vector_length() const { return vector_length_; }

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
    if (type_->IsProto() ||
        (type_->IsArray() && type_->AsArray()->element_type()->IsProto())) {
      return declared_max_length_.value_or(limits::kMaxBytesColumnLength);
    }
    return 0;
  }

  // Returns whether the column is a generated column.
  bool is_generated() const {
    return expression_.has_value() && !has_default_value_;
  }

  // Returns whether the column is an identity column.
  bool is_identity_column() const { return is_identity_column_; }

  // Returns whether the column is a placement key column.
  bool is_placement_key() const { return is_placement_key_; }

  // Returns if a generated column is stored.
  // Valid only if is_generated() is true.
  bool is_stored() const { return is_stored_; }

  // Returns whether the column has a default value.
  bool has_default_value() const { return has_default_value_; }

  // Returns the expression if the column is an evaluated column.
  const std::optional<std::string>& expression() const { return expression_; }

  // Returns the original dialect expression if the column is an evaluated
  // column.
  const std::optional<std::string>& original_expression() const {
    return original_expression_;
  }

  absl::Span<const Column* const> dependent_columns() const {
    return dependent_columns_;
  }

  const std::vector<const SchemaNode*>& sequences_used() const {
    return sequences_used_;
  }

  absl::Span<const SchemaNode* const> udf_dependencies() const {
    return udf_dependencies_;
  }

  // The locality group this column belongs to.
  const LocalityGroup* locality_group() const { return locality_group_; }

  // Returns the source column.
  const Column* source_column() const { return source_column_; }

  // Returns the table containing the column.
  const Table* table() const { return table_; }

  // Returns the list of all change streams on this column.
  absl::Span<const ChangeStream* const> change_streams() const {
    return change_streams_;
  }

  // Returns the list of all change streams explicitly tracking this column by
  // the column name.
  absl::Span<const ChangeStream* const>
  change_streams_explicitly_tracking_column() const {
    return change_streams_explicitly_tracking_column_;
  }

  bool is_trackable_by_change_stream() const { return !is_generated(); }

  // Finds a change stream on the column by its name. Name comparison is
  // case-insensitive.
  const ChangeStream* FindChangeStream(
      const std::string& change_stream_name) const;

  bool hidden() const { return hidden_; }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{.name = name_, .kind = "Column"};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("C:$0[$1]($2)$3", Name(), id_, type_->DebugString(),
                            is_deleted() ? "[DELETED]" : "");
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

  // Whether the column is a placement key column.
  bool is_placement_key_ = false;

  // A tri state boolean indicating whether commit timestamp can be stored.
  // If allows_commit_timestamp is not set, it represents that the option isn't
  // set for the columns in the schema serialized back to the user.
  std::optional<bool> allows_commit_timestamp_ = std::nullopt;

  // Length for STRING and BYTES. If unset, indicates the max allowed length.
  std::optional<int64_t> declared_max_length_ = std::nullopt;

  // Expression for an evaluated column.
  std::optional<std::string> expression_ = std::nullopt;

  // Enforce the size of a search vector. Currently it can only apply on ARRAY
  // column "Embeddings ARRAY<FLOAT64>(vector_length=>128)".
  std::optional<uint32_t> vector_length_ = std::nullopt;

  // Original dialect expression for an evaluated column.
  std::optional<std::string> original_expression_ = std::nullopt;

  // Whether the column has a default value.
  bool has_default_value_ = false;

  // Whether the column is an identity column.
  bool is_identity_column_ = false;

  // For a generated column, this is the list of columns that this column
  // references in its expression.
  std::vector<std::string> dependent_column_names_;
  std::vector<const Column*> dependent_columns_;

  // List of sequences used by this column in its expression.
  std::vector<const SchemaNode*> sequences_used_;

  // List of UDFs used by this column in its expression.
  std::vector<const SchemaNode*> udf_dependencies_;

  // If a generated column is stored. Valid only if is_generated() is true.
  bool is_stored_ = false;

  // The table containing the column.
  const Table* table_ = nullptr;

  // List of change streams referring to this column. These are owned by the
  // Schema, not by the Column.
  std::vector<const ChangeStream*> change_streams_;

  // List of change streams explicitly tracking this column by the column name.
  // These are owned by the Schema, not by the column.
  std::vector<const ChangeStream*> change_streams_explicitly_tracking_column_;

  // Indicate if the column is hidden. If true, the column will be excluded from
  // star expansion (SELECT *).
  bool hidden_ = false;

  // The locality group this column belongs to.
  const LocalityGroup* locality_group_ = nullptr;
};

// KeyColumn is a single column that is part of the key of a table or an index.
class KeyColumn : public SchemaNode {
 public:
  // Returns the column that this key column is based on.
  const Column* column() const { return column_; }

  // Returns true if this key column is sorted in descending order.
  bool is_descending() const { return is_descending_; }

  // Returns true if NULLs are sorted last in this key column.
  bool is_nulls_last() const { return is_nulls_last_; }

  // SchemaNode interface implementation.
  // ------------------------------------

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override {
    return absl::Substitute("PK:$0(desc:$1, nulls_last:$2)",
                            column_->DebugString(), is_descending_,
                            is_nulls_last_);
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
  // Whether NULLs are sorted last in this key column.
  bool is_nulls_last_ = false;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_COLUMN_H_
