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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_H_

#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Schema represents a single generation of schema of a database. It is
// a graph of SchemaNodes representing table/index/column hierarchies and
// relationships.
class Schema {
 public:
  Schema() : graph_(absl::make_unique<SchemaGraph>()) {}

  explicit Schema(std::unique_ptr<const SchemaGraph> graph);

  // Returns the generation number of this schema.
  int64_t generation() const { return generation_; }

  // Finds a table by its name. Returns a const pointer of the table, or nullptr
  // if the table is not found. Name comparison is case-insensitive.
  const Table* FindTable(const std::string& table_name) const;

  // Same as FindTable but case-sensitive.
  const Table* FindTableCaseSensitive(const std::string& table_name) const;

  // Finds an index by its name. Returns a const pointer of the index, or
  // nullptr if the index is not found. Name comparison is case-insensitive.
  const Index* FindIndex(const std::string& index_name) const;

  // List all the user-visible tables in this schema.
  absl::Span<const Table* const> tables() const { return tables_; }

  // Return the underlying SchemaGraph owning the objects in the schema.
  const SchemaGraph* GetSchemaGraph() const { return graph_.get(); }

  // Returns the number of indices in the schema.
  int num_index() const { return index_map_.size(); }

 private:
  // Manages the lifetime of all schema objects. Maintains the order
  // in which the nodes were added to the graph.
  std::unique_ptr<const SchemaGraph> graph_;

  // The generation number of this schema.
  int64_t generation_ = 0;

  // A vector that maintains the original order of tables in the DDL.
  std::vector<const Table*> tables_;

  // A map that owns all the tables. Key is the name of the tables. Hash and
  // comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Table*> tables_map_;

  // A map that owns all of the indexes. Key is the name of the index. Hash and
  // comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Index*> index_map_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_H_
