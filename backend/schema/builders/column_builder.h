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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_COLUMN_BUILDER_H
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_COLUMN_BUILDER_H

#include <memory>
#include <string>

#include "zetasql/public/type.h"
#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/validators/column_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Table;

class Column::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(new Column(
            ColumnValidator::Validate, ColumnValidator::ValidateUpdate))) {}

  std::unique_ptr<const Column> build() { return std::move(instance_); }

  const Column* get() const { return instance_.get(); }

  Builder& set_id(const ColumnID id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_type(const zetasql::Type* type) {
    instance_->type_ = type;
    return *this;
  }

  Builder& set_nullable(bool is_nullable) {
    instance_->is_nullable_ = is_nullable;
    return *this;
  }

  Builder& set_expression(const std::string& expression) {
    instance_->expression_ = expression;
    return *this;
  }

  Builder& clear_expression() {
    instance_->expression_.reset();
    return *this;
  }

  Builder& add_dependent_column_name(const std::string& column_name) {
    instance_->dependent_column_names_.push_back(column_name);
    return *this;
  }

  Builder& set_declared_max_length(absl::optional<int64_t> length) {
    instance_->declared_max_length_ = length;
    return *this;
  }

  Builder& set_table(const Table* table) {
    instance_->table_ = table;
    return *this;
  }

  Builder& set_allow_commit_timestamp(absl::optional<bool> allow) {
    instance_->allows_commit_timestamp_ = allow;
    return *this;
  }

  Builder& set_source_column(const Column* column) {
    instance_->source_column_ = column;
    instance_->type_ = column->type_;
    instance_->declared_max_length_ = column->declared_max_length_;
    return *this;
  }

 private:
  std::unique_ptr<Column> instance_;
};

class Column::Editor {
 public:
  explicit Editor(Column* instance) : instance_(instance) {}

  const Column* get() const { return instance_; }

  Editor& set_type(const zetasql::Type* type) {
    instance_->type_ = type;
    return *this;
  }

  Editor& set_nullable(bool is_nullable) {
    instance_->is_nullable_ = is_nullable;
    return *this;
  }

  Editor& set_expression(const std::string& expression) {
    instance_->expression_ = expression;
    return *this;
  }

  Editor& clear_expression() {
    instance_->expression_.reset();
    return *this;
  }

  Editor& add_dependent_column_name(const std::string& column_name) {
    instance_->dependent_column_names_.push_back(column_name);
    return *this;
  }

  Editor& set_declared_max_length(absl::optional<int64_t> length) {
    instance_->declared_max_length_ = length;
    return *this;
  }

  Editor& set_allow_commit_timestamp(absl::optional<bool> allow) {
    instance_->allows_commit_timestamp_ = allow;
    return *this;
  }

 private:
  // Not owned.
  Column* instance_;
};

class KeyColumn::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new KeyColumn(KeyColumnValidator::Validate,
                          KeyColumnValidator::ValidateUpdate))) {}

  std::unique_ptr<const KeyColumn> build() { return std::move(instance_); }

  const KeyColumn* get() const { return instance_.get(); }

  Builder& set_column(const Column* column) {
    instance_->column_ = column;
    return *this;
  }

  Builder& set_descending(bool desceding) {
    instance_->is_descending_ = desceding;
    return *this;
  }

 private:
  std::unique_ptr<KeyColumn> instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_COLUMN_BUILDER_H
