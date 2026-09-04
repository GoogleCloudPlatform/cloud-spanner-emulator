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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_QUEUE_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_QUEUE_BUILDER_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/queue.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/validators/queue_validator.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Queue::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(new Queue(
            QueueValidator::Validate, QueueValidator::ValidateUpdate))) {}

  std::unique_ptr<const Queue> build() { return std::move(instance_); }

  const Queue* get() const { return instance_.get(); }

  Builder& set_id(const QueueID& id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& add_column(const Column* column) {
    instance_->columns_.push_back(column);
    instance_->columns_map_[column->Name()] = column;
    return *this;
  }

  Builder& add_key_column(const KeyColumn* key_col) {
    instance_->primary_key_.push_back(key_col);
    return *this;
  }

  Builder& set_parent_table(const Table* table) {
    instance_->parent_table_ = table;
    return *this;
  }

  Builder& set_on_delete(Table::OnDeleteAction action) {
    instance_->on_delete_action_ = action;
    return *this;
  }

  Builder& clear_on_delete() {
    instance_->on_delete_action_ = std::nullopt;
    return *this;
  }

  Builder& set_row_deletion_policy(
      std::optional<ddl::RowDeletionPolicy> policy) {
    instance_->row_deletion_policy_ = policy;
    return *this;
  }

  Builder& set_options(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& options) {
    instance_->options_.clear();
    for (const ddl::SetOption& option : options) {
      instance_->options_.push_back(option);
    }
    return *this;
  }

 private:
  std::unique_ptr<Queue> instance_;
};

class Queue::Editor {
 public:
  explicit Editor(Queue* instance) : instance_(instance) {}

  const Queue* get() const { return instance_; }

  Editor& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Editor& add_column(const Column* column) {
    instance_->columns_.push_back(column);
    instance_->columns_map_[column->Name()] = column;
    return *this;
  }

  Editor& set_on_delete(Table::OnDeleteAction action) {
    instance_->on_delete_action_ = action;
    return *this;
  }

  Editor& clear_on_delete() {
    instance_->on_delete_action_ = std::nullopt;
    return *this;
  }

  Editor& set_row_deletion_policy(
      std::optional<ddl::RowDeletionPolicy> policy) {
    instance_->row_deletion_policy_ = policy;
    return *this;
  }

  Editor& set_options(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& options) {
    instance_->options_.clear();
    for (const ddl::SetOption& option : options) {
      instance_->options_.push_back(option);
    }
    return *this;
  }

 private:
  Queue* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_QUEUE_BUILDER_H_
