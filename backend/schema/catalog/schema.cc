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

#include "backend/schema/catalog/schema.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_node.h"
#include "re2/re2.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

const char kManagedIndexNonFingerprintRegex[] = "(IDX_\\w+_)[0-9A-F]{16}";
const int kFingerprintLength = 16;

const Table* Schema::FindTable(const std::string& table_name) const {
  auto itr = tables_map_.find(table_name);
  if (itr == tables_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Table* Schema::FindTableCaseSensitive(
    const std::string& table_name) const {
  auto table = FindTable(table_name);
  if (!table || table->Name() != table_name) {
    return nullptr;
  }
  return table;
}

const Index* Schema::FindIndex(const std::string& index_name) const {
  auto itr = index_map_.find(index_name);
  if (itr == index_map_.end()) {
    return this->FindManagedIndex(index_name);
  }
  return itr->second;
}

const Index* Schema::FindManagedIndex(const std::string& index_name) const {
  // Check that the index_name matches the format of managed index names, and
  // extract the non-fingerprint part of the index.
  std::string non_fingerprint_index_name;
  if (!RE2::FullMatch(index_name, kManagedIndexNonFingerprintRegex,
                      &non_fingerprint_index_name)) {
    return nullptr;
  }

  for (const auto& itr : index_map_) {
    if (itr.second->is_managed() &&
        itr.first.length() ==
            non_fingerprint_index_name.length() + kFingerprintLength &&
        itr.first.find(non_fingerprint_index_name) != std::string::npos) {
      return itr.second;
    }
  }
  return nullptr;
}

Schema::Schema(std::unique_ptr<const SchemaGraph> graph
               )
    : graph_(std::move(graph))
{
  tables_.clear();
  tables_map_.clear();
  index_map_.clear();
  for (const SchemaNode* node : graph_->GetSchemaNodes()) {
    const Table* table = node->As<const Table>();
    if (table != nullptr && table->is_public()) {
      tables_.push_back(table);
      tables_map_[table->Name()] = table;
      continue;
    }

    const Index* index = node->As<const Index>();
    if (index != nullptr) {
      index_map_[index->Name()] = index;
      continue;
    }

    // Columns need not be stored in the schema, they are just owned by the
    // graph.
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
