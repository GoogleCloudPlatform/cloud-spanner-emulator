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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_INDEX_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_INDEX_BUILDER_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/validators/index_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Index::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(new Index(
            IndexValidator::Validate, IndexValidator::ValidateUpdate))) {}

  std::unique_ptr<const Index> build() { return std::move(instance_); }

  const Index* get() const { return instance_.get(); }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_indexed_table(const Table* table) {
    instance_->indexed_table_ = table;
    return *this;
  }

  Builder& set_index_data_table(const Table* table) {
    instance_->index_data_table_ = table;
    return *this;
  }

  Builder& add_key_column(const KeyColumn* column) {
    instance_->key_columns_.push_back(column);
    return *this;
  }

  Builder& add_stored_column(const Column* column) {
    instance_->stored_columns_.push_back(column);
    return *this;
  }

  Builder& set_unique(bool is_unique) {
    instance_->is_unique_ = is_unique;
    return *this;
  }

  Builder& set_null_filtered(bool null_filtered) {
    instance_->is_null_filtered_ = null_filtered;
    return *this;
  }

  Builder& add_managing_node(const SchemaNode* node) {
    instance_->managing_nodes_.push_back(node);
    return *this;
  }

 private:
  std::unique_ptr<Index> instance_;
};

class Index::Editor {
 public:
  explicit Editor(Index* instance) : instance_(instance) {}

  const Index* get() const { return instance_; }

  Editor& add_managing_node(const SchemaNode* node) {
    instance_->managing_nodes_.push_back(node);
    return *this;
  }

  Editor& remove_managing_node(const SchemaNode* node) {
    auto& nodes = instance_->managing_nodes_;
    nodes.erase(std::remove(nodes.begin(), nodes.end(), node), nodes.end());
    return *this;
  }

 private:
  // Not owned.
  Index* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_INDEX_BUILDER_H_
